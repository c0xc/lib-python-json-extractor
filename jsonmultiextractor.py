"""JReader is a JSON parser for huge data structures and limited memory."""

import gzip
import tempfile
import functools
import re
from multiprocessing import Process, Pipe
from logging import debug, info, warning, error

from jsonstreamer import JSONStreamer

class JReader():
    """JReader reads a JSON file sequentially, creates a compressed copy
    and extracts certain objects from it, as specified in the schema (input).
    A jq-like query defines which objects to extract.

    Example for schema:
    [
        {"src": ".[].ID", "key": "id"},
        {"src": ".[].NAME", "key": "name"},
        {"src": ".[].TITLE", "key": "title"},
        {"src": ".[].TOKENS[]", "key": "token"}
    ]
    Each result object has the specified keys.
    """

    def __init__(self, ifile=None, *args, **kwargs):

        # Options
        self._bs = 1024*1024
        assert isinstance(self._bs, int), "blocksize must be numeric"
        assert self._bs > 0, "invalid blocksize"

        # Open input stream, copy it to compressed, temporary file (copy mode)
        # TODO file handle + rewind func?
        # TODO alternative: filename (random access fh)
        if ifile:
            # Input is a file, keep file handle
            self._mode = "file"
            self._copy_mode = True
            # TODO isinstance str / file; "tfile" ...
            #    file_handle = open(in_file, "rb")
            #    if in_file.endswith(".gz"):
            #        file_handle = gzip.open(file_handle, "rb")
            self._ifile = ifile
            # Read, copy file into compressed temp file
            self._copy_file()

        else:
            raise Exception("no input data")

    def _copy_file(self):
        # Read and copy stdin into a temporary file for random access mode
        ifile = self._ifile # stdin
        # Consume input file and write it to gzipped temporary file
        # file mode: "wb" and .encode() or explicit "wt"...
        tfile = self._temp_file = tempfile.TemporaryFile(buffering=0)
        with gzip.open(tfile, "wt") as tfile_zip:
            for b in iter(functools.partial(ifile.read, self._bs), ''):
                tfile_zip.write(b)
        # If ifile was a pipe, it would now be consumed and can be discarded
        self._ifile = None

        return self._rewind_tfile()

    def _rewind_tfile(self):
        tfile = self._temp_file
        tfile.seek(0)
        tfile_zip = gzip.open(tfile, "rt") # "rb" + .decode() or "rt"
        self._ifile_gz = tfile_zip

        return tfile_zip

    # The most difficult problem when streaming a large json file
    # is that we may have to keep reading, skipping over the following items
    # to complete the result built from the first item.
    # We would need to remember the position (path) from where
    # we began skipping over other items in order to be able to return there.
    # This is why we have our copy mode, which allows us to rewind the file
    # and restart reading it from the beginning.
    # Suppose we're extracting objects (dicts) with 3 plain values
    # and one array element...
    # .[].a, .[].b, .[].c[], .[].x
    #    |               L> get each array item under "c" separately
    #    L> get plain value, dict element "a", from (any) dict in top array
    """
    [
    {"a": "value 1", "b": "value 2", "c": ["c 1", "c 2", "c 3"], "x": "X"},
    {... "c": ["cc 1"] ...
    ...

    =>
    {"a": "value 1", "b": "value 2", "c": "c 1", "x": "X"},
    {"a": "value 1", "b": "value 2", "c": "c 2", "x": "X"},
    {"a": "value 1", "b": "value 2", "c": "c 3", "x": "X"},
    ...
    """
    # So, to build the first result object, we'd read "a", "b", then
    # the first item of "c". Now, we would need "x" but we hit another "c".
    # The next array item also matches the search condition for "c",
    # so we skip the previous value for "c" and replace it.
    # At that point, we'd have two options:
    # Approach 1) don't skip, fast-forward until we either find "x"
    # or the end of the search area (i.e., "x" does not exist)
    # then rewind, go back to the first "c", yield results and continue
    # Approach 2) skip over "c" as long as we don't have a complete result
    # at some point we'll hit "x" and be able to construct a full result
    # with "x" and the last "c", but all other items of "c" were skipped.
    # ok, approach 2 isn't a real approach, it's a way to fail.
    # Either way, we'd have to rewind, which is why we have our copy mode.

    def extract_matches(self, schema):
        """
        Generator - reads JSON input, yields result objects matching schema.

        As of now (Q1/2022), we're still using the external dependency
        JSONStreamer behind the scenes.
        Here, we're defining the callback containing the event handler
        for JSONStreamer.

        The fd is managed by this instance. In copy mode, the input data
        is streamed into a compressed, temporary file,
        which can be rewinded and read from the beginning.

        The stack, which contains the current position etc. is managed by us,
        via the callback.
        """
        # THIS IS A WORK-IN-PROGRESS!
        # (Note for later: Need to peek ahead? Then fork self, clone fd.)
        # (Note for note above: Fork required because stack contains position.)

        # Initialize stack (e_stack contains current state, position, ...)
        # This instance has a self-managed fd for the input data (copy mode)
        # The worker routine (runs outside of this instance) has JSONStreamer
        self._schema = schema # TODO move to ctor
        e_stack = self._cur_e_st = {}
        e_stack["res"] = []
        e_stack["path_l"] = [] # path elements
        e_stack["at_d"] = {} # d (depth int) => stack/pos
        # JSON event handler
        def catch_all_cb(ev, *args):
            nonlocal e_stack

            # Update stack
            self._ev_update(ev)
            self._check_clear_res() # may yield result or fail if incomplete

            # Get value
            value = None
            if len(args) > 0:
                value = args[0]

            # Node key (object)
            if ev == "key":
                assert value is not None
                self._p_node()["key"] = value

            # In fast-forward mode, skip over everything (don't return values)
            # until the specified node path is reached.
            if "ff" in e_stack:
                raise Exception("not implemented: fast-forward")
                if self._get_path() == e_stack["ff"]:
                    # arrived; to be continued
                    del e_stack["ff"]
                else:
                    return

            # Node element, value
            if ev == "value":
                # Get value => result object
                self._set_value(value) # will yield result if complete
            elif ev == "element":
                # Get value => result object
                self._set_value(value) # will yield result if complete

                # Increase index for next element
                self._p_node()["index"] += 1

        # Worker function (runs outside, context is managed here)
        def readStream(p):
            nonlocal e_stack
            e_stack["p_worker"] = p
            try:
                done = False
                while not done:
                    # Load JSON reader
                    streamer = JSONStreamer()
                    streamer.add_catch_all_listener(catch_all_cb)
                    # Reload/reset temporary (wrapped) file
                    ifile = self._rewind_tfile() # ro, no random access
                    # Dump the whole input file into the JSON reader in chunks
                    need_reset = False
                    for b in iter(functools.partial(ifile.read, self._bs), ''):
                        streamer.consume(b)
                        if "ff" in e_stack:
                            debug("reset/fast-forward requested...")
                            need_reset = True
                            break
                    streamer.close()
                    if need_reset:
                        continue
                    done = True
                p.send(None)

            except Exception as e:
                p.send(e) # we're piping an exception lol
                # otherwise: p.send(None) and raise

        # Receive results from worker
        (p_controller, p_worker) = Pipe(duplex=False)
        proc = Process(target=readStream, args=(p_worker,))
        proc.start()
        while True:
            result = p_controller.recv()
            if isinstance(result, Exception):
                proc.join()
                raise result
            #if result == "": # maybe if not isinstance(result, {})
            #    proc.join()
            #    raise Exception("parser error")
            if result is None:
                proc.join()
                break
            yield result
        p_worker.close()

    def _get_path(self, unindexed=False):
        e_stack = self._cur_e_st

        path_l = e_stack["path_l"] # path elements
        at_d = e_stack["at_d"] # d (depth int) => stack/pos
        path_str = ""
        for d, l in enumerate(path_l):
            node = at_d.get(d, {})
            if l == "OBJECT":
                # Object delimiter "."
                path_str += "."
                # Except for the top object, a key (name) is expected
                if "key" in node:
                    path_str += node["key"]
            if l == "ARRAY":
                # Object delimiter "[]"
                if unindexed: # remove indexes (produces misleading path)
                    path_str += "[]"
                else:
                    index = node["index"]
                    path_str += "[%d]" % index

        return path_str

    def _depth(self):
        e_stack = self._cur_e_st
        path_l = e_stack["path_l"] # path elements (horizontally, depth)
        return len(path_l)

    def _depth_index(self):
        return self._depth() - 1

    def _in_array(self):
        e_stack = self._cur_e_st
        path_l = e_stack["path_l"] # path elements
        return path_l[self._depth_index()] == "ARRAY"

    def _p_node(self):
        e_stack = self._cur_e_st
        at_d = e_stack["at_d"]
        node = at_d.setdefault(self._depth_index(), {}) # create missing path node
        return node

    def _ev_update(self, ev):
        e_stack = self._cur_e_st
        path_l = e_stack["path_l"] # path elements
        at_d = e_stack["at_d"]
        _di = self._depth_index
        def prune():
            for i in [d for d in at_d.keys() if d > _di()]: del at_d[i]
        # Python is almost as cool as Perl

        if ev == "doc_start":
            path_l.append("OBJECT") # it's actually a bug, but let's keep it
        elif ev == "doc_end":
            # Go one level up
            path_l.pop()
            prune() # clear position data from previous level (pop stack)
        if ev == "object_start":
            # New object
            path_l.append("OBJECT")
            at_d[self._depth_index()] = {} # new node (path) element
        elif ev == "object_end":
            # Go one level up
            path_l.pop()
            # index++ for next element
            if self._in_array():
                self._p_node()["index"] += 1
            prune() # clear position data from previous level (pop stack)
        if ev == "array_start":
            # New array
            path_l.append("ARRAY") # el type at d(epth)
            at_d[self._depth_index()] = {"index": 0} # new node (path) element
        elif ev == "array_end":
            # Go one level up
            path_l.pop()
            # index++ for next element
            if self._in_array():
                self._p_node()["index"] += 1
            prune() # clear position data from previous level (pop stack)

    def _check_clear_res(self):
        e_stack = self._cur_e_st
        pipe = e_stack["p_worker"]

        # Check if we're above all requested paths
        # Once we are, the current result object must either be empty or full;
        # new matches belong to the next result object.
        d0 = self.shortest_path_depth([req["src"] for req in self._schema])
        if self._depth() >= d0:
            return # we're within the search area for a single result
        path = self._get_path()

        # Nothing to do if current result object empty
        res_state = e_stack.get("res_state", {}) # or dummy if nothing to reset
        if not res_state.get("res"): # nothing or empty
            return

        # Check if current result object incomplete
        if not self._is_full():
            # Incomplete result, not all requested keys found
            if e_stack.get("allow_incomplete_result"):
                pipe.send(res_obj) # yield incomplete result
            else:
                raise Exception("incomplete result object at %s" % (path))

        # Check for skipped elements
        if res_state.get("skipped"):
            # TODO fast-forward not implemented
            raise Exception("skipped elements at %s (input out of order?)" % (path))
        # Reset result after we've left the req/search area
        # (full result has already been yielded in _set_value)
        if "res_state" in e_stack:
            del e_stack["res_state"]

    @staticmethod
    def shortest_path_depth(paths):
        """Determine the shortest path depth of a list of paths."""

        depth = None
        for path in paths:
            d = len(path.split("."))
            if depth is None or d < depth:
                depth = d
        return depth

    @staticmethod
    def path_matches(requested_path, path):
        """Check if requested_path pattern matches path.

        requested_path may contain a [] selector (i.e., search path)
        """

        req_path_parts = requested_path.split(".")
        path_mod_parts = path.split(".")
        if len(req_path_parts) != len(path_mod_parts):
            return False
        path_mod = None
        for i, part in enumerate(path_mod_parts):
            if path_mod is None:
                path_mod = ""
            else:
                path_mod += "."
            m = re.search(r'^(.*?)(\[\d+\])$', part)
            if m and req_path_parts[i].endswith("[]"):
                part = "%s[]" % (m.group(1))
            path_mod += part

        return requested_path == path_mod

    def _path_matches(self, requested_path, path):
        return self.path_matches(requested_path, path)

    def _is_full(self):
        e_stack = self._cur_e_st
        res_state = e_stack["res_state"]
        found_all_keys = True
        schema = self._schema
        for req_item in schema:
            if req_item["src"] not in res_state["seen"]:
                found_all_keys = False
                break
        return found_all_keys

    def _set_value(self, value):
        # Handle value, update result object...
        e_stack = self._cur_e_st
        pipe = e_stack["p_worker"]

        # Skip if current element path not on list of requested paths
        path = self._get_path()
        schema = self._schema
        req = None
        for req_item in schema:
            if self._path_matches(req_item["src"], path):
                req = req_item
                if req_item.get("key"):
                    dkey = req_item.get("key") # user-defined destination key
                else:
                    dkey = req_item["src"].split(".")[-1] # key from path
                break
        else:
            return # value not requested

        # Prepare result object, "seen" list for found keys
        # Remember path to beginning of result object
        marked_skipped = False
        def_res = {"seen": [], "skipped": [], "res": {}} # empty default
        res_state = e_stack.setdefault("res_state", def_res) # result container
        res_obj = res_state["res"]
        if not res_obj:
            e_stack["res_p0"] = self._get_path() # remember path to pos0
        req_multi = False
        if req_item["src"].endswith("[]"):
            req_multi = True # requested path may have multiple matches!
        # Check for collision (already found), fatal only for absolute path
        # NOTE we might skip items if collisions are not recognized properly
        if req["key"] in res_obj:
            if req_multi:
                # "skip element at: %s" % (path)
                res_state["skipped"].append(req["src"])
                marked_skipped = True
            else: # regular req key already found - collision
                debug("collision at %s" % (path))
                raise Exception("collision before full at %s (bad order?)" % (path))
        # Add value to result object, add path key to "seen" list for result
        res_obj[req["key"]] = value
        res_state["seen"].append(req["src"])

        # Check if result object is complete
        if self._is_full():
            if marked_skipped:
                res_state["skipped"].pop()
            pipe.send(res_obj) # yield full result
            # NOT clearing and resetting here - there could be more
            # (more matches with collected values within req area)
            # see also (for clear/yield): _check_clear_res()


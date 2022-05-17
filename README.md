JSON-EXTRACTOR
==============

This JSON parser reads a JSON stream and extracts objects matching a schema.
It streams the JSON data instead of loading it all into memory.

The problem that this parser tries to solve is:
How to extract objects (matching a schema) from a huge JSON data structure,
which is too big to be read into memory?

Say, the input file is 10GB in size and looks like this:

    [{
        "ID": 1, "NAME": "Peppe Rony", "TITLE": "", "TOKENS": [
            "token one",
            "token two",
            "token three",
            <5 million items>
        ]
    }, {
        "ID": 2, "NAME": "Garry Licky", "TITLE": "", "TOKENS": [
            "token one",
            <2 million items>
        ]
    }]

And you want to extract these objects but instead of a "TOKENS" list
(which might not fit into memory),
the object should be repeated for each list item:

    [
        {"id": 1, "name": "Peppe Rony", "title": "", "token": "token one"},
        {"id": 1, "name": "Peppe Rony", "title": "", "token": "token two"},
        {"id": 1, "name": "Peppe Rony", "title": "", "token": "token three"},
        ... <5 million items>,
        {"id": 2, "name": "Garry Licky", "title": "", "token": "token one"},
        ... <2 million items>
    ]

You would need the following schema to do extract them in that way:

    [
        {"src": ".[].ID", "key": "id"},
        {"src": ".[].NAME", "key": "name"},
        {"src": ".[].TITLE", "key": "title"},
        {"src": ".[].TOKENS[]", "key": "token"}
    ]



Dependencies
------------

This module uses this external dependency: json-streamer (by kashifrazzaqui)
> pip3 install jsonstreamer

This dependency might be removed in the future. Maybe.



Notes
-----

Each schema item describes a requested path ("src") and a destination key.
The requested path has a jq-style syntax like ".[].Foo" (to extract "Foo"
from each top-level object in the list); if the destination key ("key")
is not defined, it would be called "Foo" by default.
This source path can be a pattern describing all items in an array (...Foo[]).
Other than that, no special pattern syntax is supported at the time of writing.

This module has a "copy mode", which means that it copies all input data
to a temporary (but gz-compressed!) file. This might be required later,
to be able to rewind in case the items in the input data are out of order.
Given the example above, if "ID" comes after "TOKENS", that would be such a case.
Note that this is a special feature which should not be used accidentally.
With copy mode disabled, rewinding is not possible and parsing would fail
if the items are out of order (i.e., list items would have to be skipped).

Parsing will also fail if a requested key cannot be found,
unless incomplete results are allowed (which is probably a bad idea).

Note, again, that this tool uses json-streamer behind the scenes.
This could change in the future.



Author
------

Philip Seeger (philip@c0xc.net)



License
-------

Please see the file called LICENSE.




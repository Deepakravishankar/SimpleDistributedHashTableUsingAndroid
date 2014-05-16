SimpleDistributedHashTableUsingAndroid
======================================

Simple DHT based on Chord.

Chord paper: http://conferences.sigcomm.org/sigcomm/2001/p12-stoica.pdf

The content provider should implement all DHT functionalities. This includes all communication as well as mechanisms to handle insert/query requests and node joins.

Each content provider instance should have a node id derived from its emulator port. This node id should be obtained by applying the above hash function (i.e., genHash()) to the emulator port. For example, the node id of the content provider instance running on emulator-5554 should be, node_id = genHash(“5554”). This is necessary to find the correct position of each node in the Chord ring.

Your content provider should implement insert(), query(), and delete(). The basic interface definition is the same as the previous assignment, which allows a client app to insert arbitrary <”key”, “value”> pairs where both the key and the value are strings.

For delete(URI uri, String selection, String[] selectionArgs), you only need to use use the first two parameters, uri & selection.  This is similar to what you need to do with query().

However, please keep in mind that this “key” should be hashed by the above genHash() before getting inserted to your DHT in order to find the correct position in the Chord ring.

For your query() and delete(), you need to recognize two special strings for the selection parameter.

If “*” (a string with a single character *) is given as the selection parameter to query(), then you need to return all <key, value> pairs stored in your entire DHT.
Similarly, if “*” is given as the selection parameter to delete(), then you need to delete all <key, value> pairs stored in your entire DHT.

If “@” (a string with a single character @) is given as the selection parameter to query() on an AVD, then you need to return all <key, value> pairs stored in your local partition of the node, i.e., all <key, value> pairs stored locally in the AVD on which you run query().
Similarly, if “@” is given as the selection parameter to delete() on an AVD, then you need to delete all <key, value> pairs stored in your local partition of the node, i.e., all <key, value> pairs stored locally in the AVD on which you run delete().

An app that uses your content provider can give arbitrary <key, value> pairs, e.g., <”I want to”, “store this”>; then your content provider should hash the key via genHash(), e.g., genHash(“I want to”), get the correct position in the Chord ring based on the hash value, and store <”I want to”, “store this”> in the appropriate node.

Your content provider should implement ring-based routing. Following the design of Chord, your content provider should maintain predecessor and successor pointers and forward each request to its successor until the request arrives at the correct node. Once the correct node receives the request, it should process it and return the result (directly or recursively) to the original content provider instance that first received the request.

Your content provider do not need to maintain finger tables and implement finger-based routing. This is not required.

The first column should be named as “key” (an all lowercase string without the quotation marks). This column is used to store all keys.
The second column should be named as “value” (an all lowercase string without the quotation marks). This column is used to store all values.
All keys and values that your provider stores should use the string data type.
Note that your content provider should only store the <key, value> pairs local to its own partition.

# Hashsplit - content-based splitting of byte streams

Hashsplitting is a way of dividing a byte stream into pieces
based on the stream's content rather than on any predetermined chunk size.
As the Splitter reads the stream it maintains a rolling checksum of the last several bytes.
A chunk boundary occurs when the rolling checksum has enough trailing bits set
(where "enough" is a configurable setting that determines the average chunk size).

Hashsplitting has benefits when it comes to representing multiple,
slightly different versions of the same data.
Consider, for example, the problem of adding EXIF tags to a JPEG image file.
The tags appear near the beginning of the file, and the bulk of the image data follows.
If the file were divided into chunks at (say) 8-kilobyte boundaries,
then adding EXIF data near the beginning would alter every following chunk
(except in the lucky case where the size of the added data is an exact multiple of 8kb).
With hashsplitting, only the chunks in the vicinity of the change are affected.

Hashsplitting is used to dramatically reduce storage and bandwidth requirements
in projects like git, rsync, bup, and perkeep.

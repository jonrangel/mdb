# mdb

This is a simple excercise in implementing the MongoDB on-disk format for read-only access.
This is useful if you want to write something that reads the data off disk.
It has almost zero testing, so yeah.
I don't trust me either.

If you do use this, make sure you are not allowing writes. (fsyncAndLock()).

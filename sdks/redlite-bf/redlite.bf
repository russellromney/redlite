Redlite Brainfuck Library
=========================

This file contains helper routines for Redlite operations
Comments are anything that is not a BF instruction

MEMORY LAYOUT:
  Cell 0-7:    Syscall number and flags
  Cell 8-15:   Key buffer
  Cell 16-255: Value buffer
  Cell 256:    Regret accumulator (auto-incremented by interpreter)

SYSCALLS (executed by outputting from cell 0):
  1  = OPEN (path in cells 8+)
  2  = CLOSE
  32 = SET (key in 8+, value in 16+)
  33 = GET (key in 8+, result in 16+)
  34 = DEL (key in 8+)
  35 = EXISTS (key in 8+, result in cell 0)
  48 = INCR (key in 8+, result in 16+)
  49 = DECR (key in 8+, result in 16+)

=== OPEN DATABASE ===
Opens database at path "test.db"

Initialize syscall to 1 (OPEN)
+

Move to cell 8 for path
>>>>>>>>

Write "test.db" (ASCII: 116 101 115 116 46 100 98)
++++++++++++[->++++++++++<]>----   t (116)
>+++++++++[->+++++++++++<]>++      e (101)
>++++++++++++[->++++++++++<]>---   s (115)
>++++++++++++[->++++++++++<]>----  t (116)
>+++++[->+++++++++<]>+             . (46)
>++++++++++[->++++++++++<]>        d (100)
>+++++++++[->+++++++++++<]>-       b (98)
>                                   null terminator (0)

Go back to cell 0 and execute
<<<<<<<<<<<<<<
.

=== SET KEY VALUE ===
Sets key "hi" to value "world"

Clear and set syscall to 32 (SET)
[-]
++++++++[->++++<]>
[-]<

Move to cell 8 for key
>>>>>>>>
[-]++++++++++++[->+++++++++<]>     h (104)
>[-]++++++++++[->++++++++++<]>+    i (105)
>[-]                                null

Move to cell 16 for value
>>>
[-]++++++++++++[->+++++++++<]>---  w (119)
>[-]++++++++++[->+++++++++++<]>    o (111)
>[-]++++++++++[->+++++++++++<]>+   r (114)
>[-]++++++++++[->+++++++++++<]>    l (108)
>[-]++++++++++[->++++++++++<]>     d (100)
>[-]                                null

Go back to cell 0 and execute
<<<<<<<<<<<<<<<<<<<<<<
.

=== GET KEY ===
Gets value for key "hi" (result will be in cells 16+)

Clear and set syscall to 33 (GET)
[-]
++++++++[->++++<]>+
[-]<

Move to cell 8 for key
>>>>>>>>
[-]++++++++++++[->+++++++++<]>     h (104)
>[-]++++++++++[->++++++++++<]>+    i (105)
>[-]                                null

Go back to cell 0 and execute
<<<<<<<<<<
.

The value is now in cells 16+ Ready for output or further processing

=== INCR COUNTER ===
Increments key "counter"

Clear and set syscall to 48 (INCR)
[-]
++++++[->++++++++<]>
[-]<

Move to cell 8 for key
>>>>>>>>
[-]++++++++++[->++++++++++<]>---   c (99)
>[-]++++++++++[->+++++++++++<]>    o (111)
>[-]++++++++++[->+++++++++++<]>+++ u (117)
>[-]++++++++++[->+++++++++++<]>    n (110)
>[-]++++++++++[->+++++++++++<]>++  t (116)
>[-]++++++++++[->++++++++++<]>+    e (101)
>[-]++++++++++[->+++++++++++<]>+   r (114)
>[-]                                null

Go back to cell 0 and execute
<<<<<<<<<<<<<<<<
.

=== CLOSE DATABASE ===
Close the database connection

Clear and set syscall to 2 (CLOSE)
[-]++
.

End of library
Thank you for your suffering

(asdf:load-asd (uiop:ensure-absolute-pathname "sqlitestore.asd" (uiop:getcwd)))
(asdf:load-asd (uiop:ensure-absolute-pathname "zstd/zstd.asd" (uiop:getcwd)))
(asdf:load-system "sqlitestore")

(push (intern "MAIN" (find-package "SQLITESTORE"))
      uiop/image:*image-restore-hook*)

(uiop/image:dump-image "sqlitestore" :executable t)

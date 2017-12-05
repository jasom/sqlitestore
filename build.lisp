(asdf:load-asd (uiop:ensure-absolute-pathname "sqlitestore.asd" (uiop:load-pathname)))
(asdf:load-system "sqlitestore")

(push (intern "MAIN" (find-package "SQLITESTORE"))
      uiop/image:*image-restore-hook*)

(uiop/image:dump-image "sqlitestore" :executable t)

;;;; zstd.lisp

(in-package #:zstd)

;;; "zstd" goes here. Hacks and glory await!

(cffi:define-foreign-library libzstd
  (t (:default "libzstd")))

(cffi:use-foreign-library libzstd)

(defmacro with-foreign-buffer ((name size &key (type :uchar) initial-contents) &body b)
  `(let ((,name (cffi:foreign-alloc ,type :count ,size)))
     (unwind-protect
	  (progn
	    ,@(when initial-contents
		`((loop for idx from 0
		    for item across ,initial-contents
		      do (setf (cffi:mem-aref ,name ,type idx) item))))
	    ,@b)
       (cffi:foreign-free ,name))))
								       

(declaim (inline compress-bound))
(defun compress-bound (src-size)
  (declare (type (unsigned-byte 64)  src-size))
  (+ src-size
     (ash src-size -8)
     (if (< src-size (* 128 1024))
	 (- (* 128 1024)
	    (ash src-size -11))
	 0)))

(cffi:defcfun ("ZSTD_isError" %is-error)
    :uint (code :ulong))

(cffi:defcfun ("ZSTD_getErrorName" %error-name)
    :string (code :ulong))

(cffi:defcfun ("ZSTD_compress" %compress)
    :ulong
  (dst :pointer) (dst-capacity :ulong)
  (src :pointer) (src-size :ulong)
  (compression-level :int))

(cffi:defcfun ("ZSTD_decompress" %decompress)
    :ulong
  (dst :pointer) (dst-capacity :ulong)
  (src :pointer) (compressed-size :ulong))

(defun compress (data &optional (level 6))
  (declare (optimize (speed 3) (debug 0))
	   (type (simple-array (unsigned-byte 8) (*)) data))
  (let* ((length (length data))
	 (outbound (compress-bound length)))
    (declare (type (unsigned-byte 64) outbound))
    (with-foreign-buffer (out outbound)
      (with-foreign-buffer (in length :initial-contents data)
	(let ((result 
	       (%compress out outbound in length level)))
	  (if
	   (zerop (%is-error result))
	   (let ((returnme
		  (make-array result :element-type '(unsigned-byte 8))))
	     (loop for i from 0 below result
		  do (setf (aref returnme i)
			(cffi:mem-aref out :uint8 i)))
	     returnme)
	   (error "Error ~A while compressing" (%error-name result))))))))

(defun decompress (data max-size)
  (let ((length (length data)))
    (with-foreign-buffer (out max-size)
      (with-foreign-buffer (in length :initial-contents data)
	(let ((result (%decompress out max-size in length)))
	  (if
	   (zerop (%is-error result))
	   (let ((returnme
		  (make-array result :element-type '(unsigned-byte 8))))
	     (loop for i from 0 below result
		  do (setf (aref returnme i)
			(cffi:mem-aref out :uint8 i)))
	     returnme)
	   (error "Error ~A while decompressing" (%error-name result))))))))

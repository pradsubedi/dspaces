%module dspaces_wrapper
%{
 /* Includes the header in the wrapper code */
 #include "Python.h"
 #define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
 #include<numpy/ndarraytypes.h>
 #include "dspaces.h"
 #include "dspaces_wrapper.h"

 #define SWIG_PYTHON_STRICT_BYTE_CHAR
%}

%include "cpointer.i"
 
/* Parse the header file to generate wrappers */
%include "dspaces_wrapper.h"

%pointer_class(dspaces_client_t, dspaces_client_t);
%pointer_class(uint64_t, uint64_t);

PyObject *wrapper_dspaces_init(int rank);

void wrapper_dspaces_fini(PyObject *clientppy);

void wrapper_dspaces_kill(PyObject *clientppy);

void wrapper_dspaces_put(PyObject *clientppy, PyObject *obj, const char *name,
                         int version, PyObject *offset);

PyObject *wrapper_dspaces_get(PyObject *clientppy, const char *name,
                              int version, PyObject *lbt, PyObject *ubt,
                              PyObject *dtype, int timeout);

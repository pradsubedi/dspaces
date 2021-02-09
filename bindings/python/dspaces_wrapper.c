#include <Python.h>
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#define PY_ARRAY_UNIQUE_SYMBOL ds
#include <numpy/ndarrayobject.h>
#include <numpy/ndarraytypes.h>

#include <dspaces.h>

#include <stdio.h>

PyObject *wrapper_dspaces_init(int rank)
{
    dspaces_client_t *clientp;

    import_array();

    clientp = malloc(sizeof(*clientp));

    dspaces_init(rank, clientp);

    PyObject *client = PyLong_FromVoidPtr((void *)clientp);

    return (client);
}

void wrapper_dspaces_put(PyObject *clientppy, PyObject *obj, const char *name,
                         int version, PyObject *offset)
{
    dspaces_client_t *clientp = PyLong_AsVoidPtr(clientppy);
    PyArrayObject *arr = (PyArrayObject *)obj;
    int size = PyArray_ITEMSIZE(arr);
    int ndim = PyArray_NDIM(arr);
    void *data = PyArray_DATA(arr);
    uint64_t lb[ndim];
    uint64_t ub[ndim];
    npy_intp *shape = PyArray_DIMS(arr);
    PyObject *item;
    int i;

    for(i = 0; i < ndim; i++) {
        item = PyTuple_GetItem(offset, i);
        lb[i] = PyLong_AsLong(item);
        ub[i] = lb[i] + ((long)shape[i] - 1);
    }
    dspaces_put(*clientp, name, version, size, ndim, lb, ub, data);

    return;
}

PyObject *wrapper_dspaces_get(PyObject *clientppy, const char *name,
                              int version, PyObject *lbt, PyObject *ubt,
                              PyArray_Descr *dtype, int timeout)
{
    dspaces_client_t *clientp = PyLong_AsVoidPtr(clientppy);
    int ndim = PyTuple_GET_SIZE(lbt);
    uint64_t lb[ndim];
    uint64_t ub[ndim];
    void *data;
    PyObject *item;
    PyObject *arr;
    PyArray_Descr *descr = PyArray_DescrNew(dtype);
    npy_intp dims[ndim];
    int i;

    for(i = 0; i < ndim; i++) {
        item = PyTuple_GetItem(lbt, i);
        lb[i] = PyLong_AsLong(item);
        item = PyTuple_GetItem(ubt, i);
        ub[i] = PyLong_AsLong(item);
        dims[i] = (ub[i] - lb[i]) + 1;
    }

    dspaces_aget(*clientp, name, version, ndim, lb, ub, &data, timeout);

    arr = PyArray_NewFromDescr(&PyArray_Type, descr, ndim, dims, NULL, data, 0,
                               NULL);

    return (arr);
}

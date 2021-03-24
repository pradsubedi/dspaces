#ifndef __DSPACES_PRIVATE_H
#define __DSPACES_PRIVATE_H

#define DS_HG_REGISTER(__hgclass, __hgid, __in_t, __out_t, __handler)          \
    HG_Register(__hgclass, __hgid, BOOST_PP_CAT(hg_proc_, __in_t),             \
                BOOST_PP_CAT(hg_proc_, __out_t), _handler_for_##__handler);

#endif

#include <dspaces.h>
#include <FC.h>


void FC_GLOBAL(dspaces_init_f2c, DSPACES_INIT_F2C)(const int rank, dspaces_client_t *client, int *ierr)
{
    *ierr = dspaces_init(rank, client);
}

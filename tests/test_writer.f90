program test_writer_f
use dspaces
    type(dspaces_client) :: ndscl
    integer :: rank
    integer :: ierr

    rank = 0
    call dspaces_init(rank, ndscl, ierr)
end program test_writer_f

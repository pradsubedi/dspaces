program test_writer_f
use dspaces
    type(dspaces_client) :: ndscl
    integer :: rank
    integer :: i, ierr
    integer(kind=4), dimension(10) :: data

    rank = 0

    call dspaces_init(rank, ndscl, ierr)
    do i = 1, 10
        data(i) = i
    end do
    call dspaces_put_local(ndscl, "md0", 0_4, 0_8, 9_8, data, ierr)
    call dspaces_kill(ndscl)
    call dspaces_fini(ndscl, ierr)
end program test_writer_f

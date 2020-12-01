program test_writer_f
use dspaces
    type(dspaces_client) :: ndscl
    integer :: rank
    integer :: i, j, ierr
    integer(kind=8), dimension(2) :: lb, ub
    real(kind=8), dimension(10,10) :: data

    rank = 0

    call dspaces_init(rank, ndscl, ierr)
    do i = 1, 10
        do j = 1, 10
            data(i, j) = 2 * i + j
        end do
    end do
    lb(1) = 0
    lb(2) = 9
    ub(1) = 0
    ub(2) = 9
    call dspaces_put_local(ndscl, "md0", 0_4, lb, ub, data, ierr)
    call dspaces_kill(ndscl)
    call dspaces_fini(ndscl, ierr)
end program test_writer_f

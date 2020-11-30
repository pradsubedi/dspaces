module dspaces
    use iso_c_binding

    type dspaces_client
        type(c_ptr) :: client
    end type

contains
    subroutine dspaces_init( rank, client, ierr)
        integer, intent(in) :: rank
        type(dspaces_client), intent(out) :: client
        integer, intent(out) :: ierr
       
        call dspaces_init_f2c(rank, client%client, ierr)
    end subroutine
         
end module dspaces

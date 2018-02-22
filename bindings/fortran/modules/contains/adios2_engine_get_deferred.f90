!
! Distributed under the OSI-approved Apache License, Version 2.0.  See
!  accompanying file Copyright.txt for details.
!
!  adios2_engine_get_deferred.f90 : implementation of
!  adios2_get_deferred subroutines
!
!   Created on: Feb 21, 2018
!       Author: William F Godoy godoywf@ornl.gov
!

! Single Value
subroutine adios2_get_deferred_real(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    real, intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_dp(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    real(kind=8), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_complex(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    complex, intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_complex_dp(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    complex(kind=8), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer1(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=1), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer2(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=2), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer4(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=4), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer8(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=8), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

! 1D Array
subroutine adios2_get_deferred_real_1d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    real, dimension(:), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_dp_1d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    real(kind=8), dimension(:), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_complex_1d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    complex, dimension(:), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_complex_dp_1d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    complex(kind=8), dimension(:), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer1_1d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=1), dimension(:), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer2_1d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=2), dimension(:), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer4_1d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=4), dimension(:), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer8_1d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=8), dimension(:), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

! 2D Array
subroutine adios2_get_deferred_real_2d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    real, dimension(:, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_dp_2d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    real(kind=8), dimension(:, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_complex_2d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    complex, dimension(:, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_complex_dp_2d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    complex(kind=8), dimension(:, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer1_2d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=1), dimension(:, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer2_2d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=2), dimension(:, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer4_2d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=4), dimension(:, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer8_2d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=8), dimension(:, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

! 3D Array
subroutine adios2_get_deferred_real_3d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    real, dimension(:, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_dp_3d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    real(kind=8), dimension(:, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_complex_3d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    complex, dimension(:, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_complex_dp_3d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    complex(kind=8), dimension(:, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer1_3d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=1), dimension(:, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer2_3d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=2), dimension(:, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer4_3d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=4), dimension(:, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer8_3d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=8), dimension(:, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

! 4D Array
subroutine adios2_get_deferred_real_4d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    real, dimension(:, :, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_dp_4d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    real(kind=8), dimension(:, :, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_complex_4d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    complex, dimension(:, :, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_complex_dp_4d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    complex(kind=8), dimension(:, :, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer1_4d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=1), dimension(:, :, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer2_4d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=2), dimension(:, :, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer4_4d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=4), dimension(:, :, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer8_4d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=8), dimension(:, :, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

! 5D Array
subroutine adios2_get_deferred_real_5d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    real, dimension(:, :, :, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_dp_5d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    real(kind=8), dimension(:, :, :, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_complex_5d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    complex, dimension(:, :, :, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_complex_dp_5d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    complex(kind=8), dimension(:, :, :, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer1_5d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=1), dimension(:, :, :, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer2_5d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=2), dimension(:, :, :, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer4_5d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=4), dimension(:, :, :, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer8_5d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=8), dimension(:, :, :, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

! 6D Array
subroutine adios2_get_deferred_real_6d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    real, dimension(:, :, :, :, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_dp_6d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    real(kind=8), dimension(:, :, :, :, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_complex_6d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    complex, dimension(:, :, :, :, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_complex_dp_6d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    complex(kind=8), dimension(:, :, :, :, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer1_6d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=1), dimension(:, :, :, :, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer2_6d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=2), dimension(:, :, :, :, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer4_6d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=4), dimension(:, :, :, :, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

subroutine adios2_get_deferred_integer8_6d(engine, variable, values, ierr)
    integer(kind=8), intent(in):: engine
    integer(kind=8), intent(in):: variable
    integer(kind=8), dimension(:, :, :, :, :, :), intent(out):: values
    integer, intent(out):: ierr

    call adios2_get_deferred_f2c(engine, variable, values, ierr)

end subroutine

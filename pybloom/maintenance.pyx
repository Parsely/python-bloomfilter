'''
Cython module for fast maintenance process
'''

from libc.stdlib cimport malloc, free
from libc.math cimport sqrt, pow
import cython
cimport numpy as np

@cython.wraparound(False)
@cython.boundscheck(False)
@cython.cdivision(True)
cdef int maintenance_cyt(np.ndarray[np.uint8_t, ndim=1, mode="c"] cells, int cells_size, int num_iterations, head):

    cdef int refresh_head = head
    cdef int itr

    for itr in xrange(num_iterations):
        if cells[refresh_head] != 0:
            cells[refresh_head] -= 1
        refresh_head = (refresh_head + 1) % cells_size
    return refresh_head

def maintenance(np.ndarray[np.uint8_t, ndim=1, mode="c"] cells, int cells_size, int num_iterations, head):
    return maintenance_cyt(cells, cells_size, num_iterations, head)



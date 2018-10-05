import numpy as np
import multiprocessing
from KWBHS import *
from param_func import *


def allbh_from_txt(path_run):
    """ main function: convert all BHs data in the txt files from
        MP-Gadget into one single numpy array for each quantity """

    kwbh = KWBHS('{}{}'.format(PATH_ZOO, path_run))

    # get data from text files
    redshifts = kwbh.get_redshifts()
    bhids = kwbh.get_bhid()
    bhmasss = kwbh.get_bhmass()
    bhaccs = kwbh.get_bhacc()
    bhrhos = kwbh.get_bhrho()
    bhcss = kwbh.get_bhcs()
    bhvels = kwbh.get_bhvel()
    bhxs = kwbh.get_bhpos(0)
    bhys = kwbh.get_bhpos(1)
    bhzs = kwbh.get_bhpos(2)

    # create directory and output data
    dir_name = 'allbhdata/{}_{}/'.format(path_run[IDX_P1_I:IDX_P1_F],
                                         path_run[IDX_P2_I:IDX_P2_F])
    create_dir(dir_name)
    npsaves(dir_name, redshifts, bhmasss, bhids, bhaccs,
            bhrhos, bhcss, bhvels, bhxs, bhys, bhzs)


p = multiprocessing.Pool(16)
out = p.map(allbh_from_txt, PATH_RUNS)
p.close()

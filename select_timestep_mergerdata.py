import numpy as np
import multiprocessing
from param_func import *


def mergers_from_txt(path_run):
    """ main function: find merger tree for the most massive BHs """

    mmbh_dir = 'mmbhdata/{}_{}/'.format(path_run[IDX_P1_I:IDX_P1_F],
                                        path_run[IDX_P2_I:IDX_P2_F])
    allbh_dir = 'allbhdata/{}_{}/'.format(path_run[IDX_P1_I:IDX_P1_F],
                                          path_run[IDX_P2_I:IDX_P2_F])

    # get merger id from mmbh ids
    mmbhids = np.load('{}bhid.npy'.format(mmbh_dir))
    mgids = np.unique(mmbhids)

    # load all bh data
    redshifts = np.load('{}redshift.npy'.format(allbh_dir))
    bhmasss = np.load('{}bhmass.npy'.format(allbh_dir))
    bhaccs = np.load('{}bhacc.npy'.format(allbh_dir))
    bhids = np.load('{}bhid.npy'.format(allbh_dir))
    bhrhos = np.load('{}bhrho.npy'.format(allbh_dir))
    bhcss = np.load('{}bhcs.npy'.format(allbh_dir))
    bhvels = np.load('{}bhvel.npy'.format(allbh_dir))
    bhxs = np.load('{}bhx.npy'.format(allbh_dir))
    bhys = np.load('{}bhy.npy'.format(allbh_dir))
    bhzs = np.load('{}bhz.npy'.format(allbh_dir))

    # merger bhid survive
    redshift = by_merger_id(mgids, bhids, redshifts)
    bhmass = by_merger_id(mgids, bhids, bhmasss)
    bhacc = by_merger_id(mgids, bhids, bhaccs)
    bhid = by_merger_id(mgids, bhids, bhids)
    bhrho = by_merger_id(mgids, bhids, bhrhos)
    bhcs = by_merger_id(mgids, bhids, bhcss)
    bhvel = by_merger_id(mgids, bhids, bhvels)
    bhx = by_merger_id(mgids, bhids, bhxs)
    bhy = by_merger_id(mgids, bhids, bhys)
    bhz = by_merger_id(mgids, bhids, bhzs)

    # create directory and output data
    dir_name = 'mergerdata/{}_{}/'.format(path_run[IDX_P1_I:IDX_P1_F],
                                          path_run[IDX_P2_I:IDX_P2_F])
    create_dir(dir_name)
    npsaves(dir_name, redshift, bhmass, bhid, bhacc,
            bhrho, bhcs, bhvel, bhx, bhy, bhz)


p = multiprocessing.Pool(16)
out = p.map(mergers_from_txt, PATH_RUNS)
p.close()

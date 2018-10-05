import numpy as np
import multiprocessing
from KWBHS import *
from param_func import *


def mmbh_from_txt(path_run):
    """ main function: convert most massive BHs data in the txt files
        from MP-Gadget into one single numpy array for each quantity """

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

    # get quantity of the most massive BH at each time step
    redshift = np.unique(redshifts)
    bhmass = get_mmbharr(bhmasss, bhmasss, redshifts)
    bhid = get_mmbharr(bhids, bhmasss, redshifts)
    bhacc = get_mmbharr(bhaccs, bhmasss, redshifts)
    bhrho = get_mmbharr(bhrhos, bhmasss, redshifts)
    bhcs = get_mmbharr(bhcss, bhmasss, redshifts)
    bhvel = get_mmbharr(bhvels, bhmasss, redshifts)
    bhx = get_mmbharr(bhxs, bhmasss, redshifts)
    bhy = get_mmbharr(bhys, bhmasss, redshifts)
    bhz = get_mmbharr(bhzs, bhmasss, redshifts)

    # mask arr of the most massive BH at each time step
    mask = filt_bhmass(bhmass)

    # select output final data
    redshift = redshift[mask]
    bhmass = bhmass[mask]
    bhid = bhid[mask]
    bhacc = bhacc[mask]
    bhrho = bhrho[mask]
    bhcs = bhcs[mask]
    bhvel = bhvel[mask]
    bhx = bhx[mask]
    bhy = bhy[mask]
    bhz = bhz[mask]

    # create directory and output data
    dir_name = 'mmbhdata/{}_{}/'.format(path_run[IDX_P1_I:IDX_P1_F],
                                        path_run[IDX_P2_I:IDX_P2_F])
    create_dir(dir_name)
    npsaves(dir_name, redshift, bhmass, bhid, bhacc,
            bhrho, bhcs, bhvel, bhx, bhy, bhz)


p = multiprocessing.Pool(16)
out = p.map(mmbh_from_txt, PATH_RUNS)
p.close()

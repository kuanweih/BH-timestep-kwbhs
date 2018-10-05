import numpy as np
import multiprocessing
from KWBHS import *


PATH_ZOO = '/physics2/kuanweih/project_BH_seedmass/simulation_zoo/'
PATH_RUNS = ['run_10Mpc_362341/Run_seed5e5/Con_2/']    # list of runs
# TODO should I use list comprehension here?
# TODO PATH_RUN[:16] will give 'run_10Mpc_362341'
# TODO PATH_RUN[17:-7] will give 'Run_seed5e5'
# TODO PATH_RUN[25:-7] will give '5e5'


def npsaves(dir_name, redshifts, bhmasss, bhids, bhaccs,
            bhrhos, bhcss, bhvels, bhxs, bhys, bhzs):
    """ save bh data """
    np.save('{}redshift'.format(dir_name), redshifts)
    np.save('{}bhmass'.format(dir_name), bhmasss)
    np.save('{}bhid'.format(dir_name), bhids)
    np.save('{}bhacc'.format(dir_name), bhaccs)
    np.save('{}bhrho'.format(dir_name), bhrhos)
    np.save('{}bhcs'.format(dir_name), bhcss)
    np.save('{}bhvel'.format(dir_name), bhvels)
    np.save('{}bhx'.format(dir_name), bhxs)
    np.save('{}bhy'.format(dir_name), bhys)
    np.save('{}bhz'.format(dir_name), bhzs)


def create_dir(dir_name):
    """ create directory name according to the run """
    import os
    import errno
    if not os.path.exists(os.path.dirname(dir_name)):
        try:
            os.makedirs(os.path.dirname(dir_name))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise


def mmbh_from_txt(path_run):

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

    def get_mmbhele(arr, bhmass, redshift, z):
        con = redshift == z
        bhmass_z = bhmass[con]
        arr_z = arr[con]
        mmbhele = arr_z[np.argmax(bhmass_z)]
        return mmbhele

    def get_mmbharr(arr, bhmass, redshift):
        mmbharr = np.array([get_mmbhele(arr, bhmass, redshift, z)
                            for z in np.unique(redshifts)])
        return mmbharr

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

    def filt_bhmass(bhmass):
        bhmass_sort = np.flip(bhmass, 0)
        mask = [True] * len(bhmass)
        for i in range(1, len(bhmass)):
            if bhmass_sort[i - 1] > bhmass_sort[i]:
                bhmass_sort[i] = bhmass_sort[i - 1]
                mask[i] = False
        return np.flip(mask, 0)

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
    dir_name = 'mmbhdata/{}_{}/'.format(path_run[:16], path_run[25:-7])
    create_dir(dir_name)
    npsaves(dir_name, redshifts, bhmasss, bhids, bhaccs,
            bhrhos, bhcss, bhvels, bhxs, bhys, bhzs)


p = multiprocessing.Pool(16)
out = p.map(mmbh_from_txt, PATH_RUNS)
p.close()

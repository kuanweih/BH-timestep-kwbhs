import numpy as np
import multiprocessing
from KWBHS import *


PATH_ZOO = '/physics2/kuanweih/project_BH_seedmass/simulation_zoo/'
PATH_RUNS = ['run_10Mpc_362341/Run_seed5e5/Con_2/']    # list of runs
# TODO should I use list comprehension here?
# TODO PATH_RUN[:16] will give 'run_10Mpc_362341'
# TODO PATH_RUN[17:-7] will give 'Run_seed5e5'
# TODO PATH_RUN[25:-7] will give '5e5'


def npsaves(dir_name):
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
    dir_name = 'allbhdata/{}_{}/'.format(path_run[:16], path_run[25:-7])
    create_dir(dir_name)
    npsaves(dir_name)


p = multiprocessing.Pool(16)
out = p.map(allbh_from_txt, PATH_RUNS)
p.close()

import numpy as np
import multiprocessing


PATH_ZOO = '/physics2/kuanweih/project_BH_seedmass/simulation_zoo/'
PATH_RUNS = ['run_10Mpc_362341/Run_seed5e5/Con_2/']    # list of runs
# TODO should I use list comprehension here?
# TODO PATH_RUN[:16] will give 'run_10Mpc_362341'
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


def mergers_from_txt(path_run):

    mmbh_dir = 'mmbhdata/{}_{}/'.format(path_run[:16], path_run[25:-7])
    allbh_dir = 'allbhdata/{}_{}/'.format(path_run[:16], path_run[25:-7])

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

    def by_merger_id(mgids, bhids, quantity):
        con = np.array([False] * len(bhids))
        for v in range(len(mgids)):
            con = con + (bhids == mgids[v])
        return quantity[con]

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
    dir_name = 'mergerdata/{}_{}/'.format(path_run[:16], path_run[25:-7])
    create_dir(dir_name)
    npsaves(dir_name, redshifts, bhmasss, bhids, bhaccs,
            bhrhos, bhcss, bhvels, bhxs, bhys, bhzs)


p = multiprocessing.Pool(16)
out = p.map(mergers_from_txt, PATH_RUNS)
p.close()

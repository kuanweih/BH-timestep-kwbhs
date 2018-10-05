import numpy as np

# simulation dir
PATH_ZOO = '/physics2/kuanweih/project_BH_seedmass/simulation_zoo/'
PATH_RUNS = ['run_10Mpc_362341/Run_seed5e5/Con_2/']    # list of runs

# e.g. 'run_10Mpc_362341'
IDX_P1_I = 0
IDX_P1_F = 16
# e.g. '5e5'
IDX_P2_I = 25
IDX_P2_F = -7


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


def get_mmbhele(arr, bhmass, redshift, z):
    con = redshift == z
    bhmass_z = bhmass[con]
    arr_z = arr[con]
    mmbhele = arr_z[np.argmax(bhmass_z)]
    return mmbhele


def by_merger_id(mgids, bhids, quantity):
    con = np.array([False] * len(bhids))
    for v in range(len(mgids)):
        con = con + (bhids == mgids[v])
    return quantity[con]


def get_mmbharr(arr, bhmass, redshift):
    mmbharr = np.array([get_mmbhele(arr, bhmass, redshift, z)
                        for z in np.unique(redshift)])
    return mmbharr


def filt_bhmass(bhmass):
    bhmass_sort = np.flip(bhmass, 0)
    mask = [True] * len(bhmass)
    for i in range(1, len(bhmass)):
        if bhmass_sort[i - 1] > bhmass_sort[i]:
            bhmass_sort[i] = bhmass_sort[i - 1]
            mask[i] = False
    return np.flip(mask, 0)

import numpy as np
import multiprocessing
from KWBHS import *


h = 0.697

# main function


def mmbh_from_txt(realization_seed):
    # path_run = '/physics2/kuanweih/project_BH_seedmass/simulation_zoo/run_10Mpc_{0}/Con_2/'.format(realization_seed)
    path_run = '/physics2/kuanweih/project_BH_seedmass/simulation_zoo/run_15Mpc_{}/Con_2/'.format(
        realization_seed)
    # path_run = '/nfs/nas-0-1/kuanweih/simulation_zoo/run_15Mpc_{0}/Con_2/'.format(realization_seed)
    # path_run = '/physics2/kuanweih/project_BH_seedmass/simulation_zoo/run_20Mpc_{0}/Con_2/'.format(realization_seed)
    kwbh = KWBHS(path_run)

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

    # create directory name according to the run
    import os
    import errno
    # dir_name = 'mmbhdata/15Mpc_{0}_{1}_high396/'.format(realization_seed[:6],realization_seed[-3:])
    # dir_name = 'mmbhdata/15Mpc_{0}_{1}_newcode/'.format(realization_seed[:6],realization_seed[-3:])
    # dir_name = 'mmbhdata/15Mpc_{0}_{1}_samefof/'.format(realization_seed[:6],realization_seed[-3:])
    dir_name = 'mmbhdata/15Mpc_{}_{}/'.format(
        realization_seed[:6], realization_seed[-3:])
    # dir_name = 'mmbhdata/10Mpc_{0}_{1}/'.format(realization_seed[:6],realization_seed[-3:])
    if not os.path.exists(os.path.dirname(dir_name)):
        try:
            os.makedirs(os.path.dirname(dir_name))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    # save date
    np.save('{}redshift'.format(dir_name), redshift)
    np.save('{}bhmass'.format(dir_name), bhmass)
    np.save('{}bhid'.format(dir_name), bhid)
    np.save('{}bhacc'.format(dir_name), bhacc)
    np.save('{}bhrho'.format(dir_name), bhrho)
    np.save('{}bhcs'.format(dir_name), bhcs)
    np.save('{}bhvel'.format(dir_name), bhvel)
    np.save('{}bhx'.format(dir_name), bhx)
    np.save('{}bhy'.format(dir_name), bhy)
    np.save('{}bhz'.format(dir_name), bhz)


# runs = ['090585/Run_seed5e5',
#         '181170/Run_seed5e5',
#         '271755/Run_seed5e5',
#         '362340/Run_seed5e5',
#         '452925/Run_seed5e5',
#         '543510/Run_seed5e5',
#         '634095/Run_seed5e5',
#         '724680/Run_seed5e5',
#         '815265/Run_seed5e5',
#         '905850/Run_seed5e5',
#
#         '090586/Run_seed5e5',
#         '181171/Run_seed5e5',
#         '271756/Run_seed5e5',
#         '362341/Run_seed5e5',
#         '452926/Run_seed5e5',
#         '543511/Run_seed5e5',
#         '634096/Run_seed5e5',
#         '724681/Run_seed5e5',
#         '815266/Run_seed5e5',
#         '905851/Run_seed5e5']

# runs = ['271755/Run_seed5e3',

#         '362341/Run_seed5e3',
#         '543511/Run_seed5e3']

# runs = ['271755/Run_seed5e4',

#        '362341/Run_seed5e4',
#        '543511/Run_seed5e4']

# runs = ['543511/Run_seed5e3','543511/Run_seed5e5',
#        '815265/Run_seed5e3','815265/Run_seed5e5']

# runs = ['543511/Run_seed5e3','543511/Run_seed5e4','543511/Run_seed5e5']
# runs = ['543511_396/Run_seed5e3','543511_396/Run_seed5e5']
# runs = ['543511/Run_seed5e5']

# runs = ['181170/Run_seed5e5',
#         '271755/Run_seed5e5',
#         '362341/Run_seed5e5',
#         '543511/Run_seed5e5',
#         '815265/Run_seed5e5']

runs = ['362341/Run_seed5e5']

# runs = ['543511_samefofmass/Run_seed5e4']
# runs = ['543511_samefofmass/Run_seed5e3']
# runs = ['543511_new/Run_seed5e5']
# runs = ['543511_396/Run_seed5e5','543511_396/Run_seed5e3']


p = multiprocessing.Pool(16)
out = p.map(mmbh_from_txt, runs)
p.close()

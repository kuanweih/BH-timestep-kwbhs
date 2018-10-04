import numpy as np
import multiprocessing
from KWBHS import *

PATH_ZOO = '/physics2/kuanweih/project_BH_seedmass/simulation_zoo/'
PATH_RUN = ['run_10Mpc_362341/Run_seed5e5/Con_2/']
#TODO should I use list comprehension here?
#TODO PATH_RUN[:16] will give 'run_10Mpc_362341'
#TODO PATH_RUN[17:-7] will give 'Run_seed5e5'

runs = ['362341/Run_seed5e5']
dir_name = 'allbhdata/10Mpc_{}_{}/'.format(
    realization_seed[:6], realization_seed[-3:])

def allbh_from_txt(realization_seed):
    path_run = '/physics2/kuanweih/project_BH_seedmass/simulation_zoo/run_10Mpc_{}/Con_2/'.format(
        realization_seed)
    # path_run = '/physics2/kuanweih/project_BH_seedmass/simulation_zoo/run_10Mpc_{}/Con_2/'.format(realization_seed)
    # path_run = '/nfs/nas-0-1/kuanweih/simulation_zoo/run_15Mpc_{}/Con_2/'.format(realization_seed)
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

    # create directory name according to the run
    import os
    import errno
    # dir_name = 'allbhdata/15Mpc_{}_{}_high396/'.format(realization_seed[:6],realization_seed[-3:])
    # dir_name = 'allbhdata/15Mpc_{}_{}_newcode/'.format(realization_seed[:6],realization_seed[-3:])
    # dir_name = 'allbhdata/15Mpc_{}_{}_samefof/'.format(realization_seed[:6],realization_seed[-3:])
    dir_name = 'allbhdata/10Mpc_{}_{}/'.format(
        realization_seed[:6], realization_seed[-3:])
    # dir_name = 'allbhdata/10Mpc_{}_{}/'.format(realization_seed[:6],realization_seed[-3:])
    if not os.path.exists(os.path.dirname(dir_name)):
        try:
            os.makedirs(os.path.dirname(dir_name))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    # save date
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


# runs = ['181170/Run_seed5e5',
#         '271755/Run_seed5e5',
#         '362341/Run_seed5e5',
#         '543511/Run_seed5e5',
#         '815265/Run_seed5e5']
runs = ['362341/Run_seed5e5']

# runs = ['543511_396/Run_seed5e3']

# runs = ['543511_samefofmass/Run_seed5e4']
# runs = ['543511_samefofmass/Run_seed5e3']
# runs = ['543511_new/Run_seed5e5']
# runs = ['543511_396/Run_seed5e5','543511_396/Run_seed5e3']


p = multiprocessing.Pool(16)
out = p.map(allbh_from_txt, runs)
p.close()

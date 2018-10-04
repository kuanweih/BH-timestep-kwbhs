import numpy as np
import multiprocessing
# from KWBHS import *


h = 0.697

# main function


def mergers_from_txt(realization_seed):
    rs = realization_seed

    # get merger id from mmbh ids
    mmbhids = np.load('mmbhdata/{}_{}/bhid.npy'.format(rs[:12], rs[-3:]))
    mgids = np.unique(mmbhids)

    # load all bh data
    redshifts = np.load(
        'allbhdata/{}_{}/redshift.npy'.format(rs[:12], rs[-3:]))
    bhmasss = np.load('allbhdata/{}_{}/bhmass.npy'.format(rs[:12], rs[-3:]))
    bhaccs = np.load('allbhdata/{}_{}/bhacc.npy'.format(rs[:12], rs[-3:]))
    bhids = np.load('allbhdata/{}_{}/bhid.npy'.format(rs[:12], rs[-3:]))
    bhrhos = np.load('allbhdata/{}_{}/bhrho.npy'.format(rs[:12], rs[-3:]))
    bhcss = np.load('allbhdata/{}_{}/bhcs.npy'.format(rs[:12], rs[-3:]))
    bhvels = np.load('allbhdata/{}_{}/bhvel.npy'.format(rs[:12], rs[-3:]))
    bhxs = np.load('allbhdata/{}_{}/bhx.npy'.format(rs[:12], rs[-3:]))
    bhys = np.load('allbhdata/{}_{}/bhy.npy'.format(rs[:12], rs[-3:]))
    bhzs = np.load('allbhdata/{}_{}/bhz.npy'.format(rs[:12], rs[-3:]))

    # # get merger id from mmbh ids
    # mmbhids = np.load('mmbhdata/{}_{}_samefof/bhid.npy'.format(rs[:12],rs[-3:]))
    # mmbhids = np.load('mmbhdata/{}_{}_high396/bhid.npy'.format(rs[:12],rs[-3:]))
    # mgids = np.unique(mmbhids)

    # load all bh data
    # redshifts = np.load('allbhdata/{}_{}_high396/redshift.npy'.format(rs[:12],rs[-3:]))
    # bhmasss   = np.load('allbhdata/{}_{}_high396/bhmass.npy'.format(rs[:12],rs[-3:]))
    # bhaccs    = np.load('allbhdata/{}_{}_high396/bhacc.npy'.format(rs[:12],rs[-3:]))
    # bhids     = np.load('allbhdata/{}_{}_high396/bhid.npy'.format(rs[:12],rs[-3:]))
    # bhrhos    = np.load('allbhdata/{}_{}_high396/bhrho.npy'.format(rs[:12],rs[-3:]))
    # bhcss     = np.load('allbhdata/{}_{}_high396/bhcs.npy'.format(rs[:12],rs[-3:]))
    # bhvels    = np.load('allbhdata/{}_{}_high396/bhvel.npy'.format(rs[:12],rs[-3:]))
    # bhxs      = np.load('allbhdata/{}_{}_high396/bhx.npy'.format(rs[:12],rs[-3:]))
    # bhys      = np.load('allbhdata/{}_{}_high396/bhy.npy'.format(rs[:12],rs[-3:]))
    # bhzs      = np.load('allbhdata/{}_{}_high396/bhz.npy'.format(rs[:12],rs[-3:]))

    # load all bh data
    # redshifts = np.load('allbhdata/{}_{}_samefof/redshift.npy'.format(rs[:12],rs[-3:]))
    # bhmasss   = np.load('allbhdata/{}_{}_samefof/bhmass.npy'.format(rs[:12],rs[-3:]))
    # bhaccs    = np.load('allbhdata/{}_{}_samefof/bhacc.npy'.format(rs[:12],rs[-3:]))
    # bhids     = np.load('allbhdata/{}_{}_samefof/bhid.npy'.format(rs[:12],rs[-3:]))
    # bhrhos    = np.load('allbhdata/{}_{}_samefof/bhrho.npy'.format(rs[:12],rs[-3:]))
    # bhcss     = np.load('allbhdata/{}_{}_samefof/bhcs.npy'.format(rs[:12],rs[-3:]))
    # bhvels    = np.load('allbhdata/{}_{}_samefof/bhvel.npy'.format(rs[:12],rs[-3:]))
    # bhxs      = np.load('allbhdata/{}_{}_samefof/bhx.npy'.format(rs[:12],rs[-3:]))
    # bhys      = np.load('allbhdata/{}_{}_samefof/bhy.npy'.format(rs[:12],rs[-3:]))
    # bhzs      = np.load('allbhdata/{}_{}_samefof/bhz.npy'.format(rs[:12],rs[-3:]))

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

    # create directory name according to the run
    import os
    import errno
    # dir_name = 'mergerdata/15Mpc_{}_{}_high396/'.format(realization_seed[:6],realization_seed[-3:])
    # dir_name = 'mergerdata/15Mpc_{}_{}_newcode/'.format(realization_seed[:6],realization_seed[-3:])
    # dir_name = 'mergerdata/15Mpc_{}_{}_samefof/'.format(realization_seed[:6],realization_seed[-3:])
    # dir_name = 'mergerdata/10Mpc_{}_{}_samefof/'.format(realization_seed[:6],realization_seed[-3:])
    dir_name = 'mergerdata/{}_{}/'.format(
        realization_seed[:12], realization_seed[-3:])
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


# runs = ['15Mpc_543511/Run_seed5e3','15Mpc_543511/Run_seed5e4','15Mpc_543511/Run_seed5e5']
# runs = ['15Mpc_543511/Run_seed5e5']
# runs = ['10Mpc_181170/Run_seed5e5',
#         '10Mpc_271755/Run_seed5e5',
#         '10Mpc_362341/Run_seed5e5',
#         '10Mpc_543511/Run_seed5e5',
#         '10Mpc_815265/Run_seed5e5']

runs = ['15Mpc_362341/Run_seed5e5']

# runs = ['15Mpc_543511_samefofmass/Run_seed5e4']
# runs = ['15Mpc_543511_samefofmass/Run_seed5e3']
# runs = ['15Mpc_543511_new/Run_seed5e5']
# runs = ['15Mpc_543511_396/Run_seed5e5','15Mpc_543511_396/Run_seed5e3']


p = multiprocessing.Pool(16)
out = p.map(mergers_from_txt, runs)
p.close()

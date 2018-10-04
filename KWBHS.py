import numpy as np
import glob

# 0  All.Time, <br>
# 1  P[i].ID, <br>
# 2  BHP(i).Mass, <br>
# 3  mdot, <br>
# 4  rho_proper,
# 5  soundspeed,<br>
# 6  bhvel, <br>
# 7  P[i].Pos[0], <br>
# 8  P[i].Pos[1], <br>
# 9  P[i].Pos[2]);


class KWBHS:

    def __init__(self, path_run):
        self.path_run = path_run

    def together_txt(self):
        txt_paths = glob.glob('{0}kwbhs/kwblackholes*'.format(self.path_run))
        txts = np.array([np.loadtxt(txt_path) for txt_path in txt_paths])
        txt = np.concatenate([txts[u] for u in range(len(txts))], axis=0)
        return txt

    def get_redshifts(self):
        timesteps = self.together_txt()[:, 0]       # scale factors
        redshifts = 1. / timesteps - 1.
        return redshifts

    def get_bhid(self):
        bhid = self.together_txt()[:, 1]
        return bhid

    def get_bhmass(self):
        bhmass = self.together_txt()[:, 2]
        return bhmass

    def get_bhacc(self):
        bhacc = self.together_txt()[:, 3]
        return bhacc

    def get_bhrho(self):
        bhrho = self.together_txt()[:, 4]
        return bhrho

    def get_bhcs(self):
        bhcs = self.together_txt()[:, 5]
        return bhcs

    def get_bhvel(self):
        bhvel = self.together_txt()[:, 6]
        return bhvel

    def get_bhpos(self, xyz):
        bhpos = self.together_txt()[:, xyz + 7]
        return bhpos

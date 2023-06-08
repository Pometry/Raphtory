import matplotlib.pyplot as plt
import numpy as np
import distinctipy

from matplotlib.offsetbox import OffsetImage,AnnotationBbox

# Mapping different motifs to their place in the heatmap.

mapper = {0:(5,5),
    1:(5,4),
    2:(4,5),
    3:(4,4),
    4:(4,3),
    5:(4,2),
    6:(5,3),
    7:(5,2),
    8:(0,0),
    9:(0,1),
    10:(1,0),
    11:(1,1),
    12:(2,1),
    13:(2,0),
    14:(3,1),
    15:(3,0),
    16:(0,5),
    17:(0,4),
    18:(1,5),
    19:(1,4),
    20:(2,3),
    21:(2,2),
    22:(3,3),
    23:(3,2),
    24:(5,0),
    25:(5,1),
    26:(4,0),
    27:(4,1),
    28:(4,1),
    29:(4,0),
    30:(5,1),
    31:(5,0),
    32:(0,2),
    33:(2,4),
    34:(1,2),
    35:(1,3),
    36:(0,3),
    37:(2,5),
    38:(3,4),
    39:(3,5)}

def to_3d_heatmap(motif_flat, data_type=int):
    motif_3d = np.zeros((6,6),dtype=data_type)
    for i in range(40):
        motif_3d[mapper[i]]=motif_flat[i]
    return motif_3d

def human_format(num):
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    # add more suffixes if you need them
    return '%.1f%s' % (num, ['', 'K', 'M', 'B', 'T', 'P'][magnitude])

def get_labels(motif_map):
    return np.vectorize(human_format)(motif_map)

def get_motif(xory:str,y:int):
    path = "../motif-pics/"+xory+str(y)+".png"
    return plt.imread(path)

def offset_image(xory, coord, name, ax):
    img = get_motif(xory, name)
    im = OffsetImage(img,zoom=0.04)
    im.image.axes = ax

    if(xory=="x"):
        ab = AnnotationBbox(im, (coord+0.5, 5.5),  xybox=(0., -40.), frameon=False,
                            xycoords='data',  boxcoords="offset points", box_alignment=(0.5,0.5), pad=0)

    else:
        ab = AnnotationBbox(im, (0, coord),  xybox=(0., -40.), frameon=False,
                            xycoords='data',  boxcoords="offset points", box_alignment=(1.0,0.0), pad=0)

    ax.add_artist(ab) 

# For making CDFs and CCDFs

def cdf(listlike, normalised=True):
    data = np.array(listlike)
    N = len(listlike)

    x = np.sort(data)
    if (normalised):
        y = np.arange(N)/float(N-1)
    else:
        y = np.arange(N)
    return x, y

def ccdf(listlike, normalised=True):
    x, y = cdf(listlike,normalised)
    if normalised:
        return x, 1.0-y
    else:
        return x, len(listlike)-y

def lorenz(listlike): 
    tmp_arr = np.array(sorted(listlike))
    # print(tmp_arr[0])
    x= np.arange(listlike.size)/(listlike.size -1)
    y = tmp_arr.cumsum() / tmp_arr.sum()
    return x,y
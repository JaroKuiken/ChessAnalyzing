import matplotlib.pyplot as plt
import numpy as np
from matplotlib import colors
from matplotlib.colors import LogNorm

#Initialize data struct
nrows, ncols = 8, 8
data = np.zeros(nrows*ncols)

#Assemble data
queen_data = {'f1': 541278, 'f2': 591685, 'f3': 4437838, 'f4': 889180, 'd8': 114019, 'f6': 1303599, 'f7': 172600, 'f8': 478050, 'h3': 675676, 'h1': 81477, 'h6': 396367, 'h7': 52361, 'h4': 294079, 'h5': 174407, 'b4': 694542, 'b5': 704922, 'b6': 448840, 'b7': 930230, 'b1': 162667, 'b2': 338608, 'b3': 349575, 'd6': 2095824, 'd7': 1529859, 'd4': 1488490, 'd5': 1498222, 'd2': 1035978, 'd3': 1189394, 'f5': 1556342, 'b8': 42893, 'e5': 1913738, 'c2': 191673, 'h2': 209022, 'e3': 1132131, 'c7': 179067, 'h8': 30875, 'd1': 304852, 'g7': 1402420, 'g6': 726222, 'g5': 1080569, 'g4': 1038050, 'g3': 618953, 'g2': 737445, 'g1': 83098, 'g8': 21609, 'a1': 257545, 'a3': 389174, 'a2': 96127, 'a5': 108149, 'a4': 148439, 'a7': 80508, 'a6': 455030, 'c3': 3292828, 'e4': 1464814, 'c1': 243356, 'e6': 2303980, 'e1': 155738, 'c6': 831263, 'c5': 1468347, 'c4': 793719, 'e7': 2123628, 'a8': 51752, 'c8': 517334, 'e2': 1086108, 'e8': 92609}
for i, key in enumerate(sorted(queen_data)):
    data[i] = queen_data[key]
    print(f'Amt capts: {data[i]}, sq: {key}')
data = data.astype(int).reshape(8,8)
data = np.rot90(data)
print(data)

#Figure labels
row_labels = [8,7,6,5,4,3,2,1]
col_labels = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']

#Plot figure
plt.matshow(data, cmap='copper_r', norm=LogNorm())
plt.xticks(range(ncols), col_labels)
plt.yticks(range(nrows), row_labels)
plt.colorbar()
plt.title('The Black Bishops died most often at:')
plt.show()

import matplotlib.pyplot as plt
#for plotting
import numpy as np 

# create plot
fig, ax = plt.subplots()
bar_width = 0.15
opacity = 1

xlabel= np.array([10, 100, 1000, 10000])

index = np.arange(4)
postgresVals = [-1976824/1048576.0, -462352/1048576.0, 14900536/1048576.0, 70299296/1048576.0]
hdfsVals = [3531984/1048576.0, 4264640/1048576.0, 15046824/1048576.0, 145431280/1048576.0]



plt.bar(index, hdfsVals, bar_width,
            alpha=opacity,
            color='#595959',
            label='HDFS')
    
plt.bar(index + bar_width, postgresVals, bar_width,
            alpha=opacity,
            color='#F6921E',
            label='HDFS-Postgres')

ax.set_xticks(index + bar_width / 2)
ax.set_xticklabels(xlabel)
# ax.set_yticklabels(ylabel)

#plt.yscale('log')
plt.ylim(-10, 140)
plt.axhline(y = 0, linestyle='--', color='black', linewidth=1)
plt.ylabel('Memory Used (# MBytes)')
plt.xlabel('# Open Files')
ttlStr = ''
plt.title(ttlStr)
plt.legend()
    
plt.tight_layout()
plt.show()
fig.savefig("mem.pdf", bbox_inches='tight')
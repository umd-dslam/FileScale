
import matplotlib.pyplot as plt
#for plotting
import numpy as np 

# create plot
fig, ax = plt.subplots()
bar_width = 0.15
opacity = 1

xlabel= np.array([10, 100, 1000, 10000])

index = np.arange(4)
postgresVals = [121.95, 215.98, 203.54, 71.75]
hdfsVals = [227.27, 925.92, 1851.85, 3299.24]



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
# plt.ylim(-10, 140)
plt.ylabel('# Ops per sec')
plt.xlabel('# Open Files')
ttlStr = ''
plt.title(ttlStr)
plt.legend()
    
plt.tight_layout()
plt.show()

fig.savefig("ops.pdf", bbox_inches='tight')
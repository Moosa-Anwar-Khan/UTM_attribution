import matplotlib.pyplot as plt
from pathlib import Path

def chart_bar(df, x, y, title, xlab, ylab, outpath: Path):
    plt.figure()
    plt.bar(df[x], df[y])
    plt.title(title)
    plt.xlabel(xlab)
    plt.ylabel(ylab)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    outpath.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(outpath)
    plt.close()

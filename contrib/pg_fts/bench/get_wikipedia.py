import sys, pyarrow.parquet as pq
from huggingface_hub import list_repo_files, hf_hub_download

REPO="wikimedia/wikipedia"; CONFIG="20231101.en"
files=[f for f in list_repo_files(REPO, repo_type="dataset") if f.startswith(CONFIG+"/") and f.endswith(".parquet")]
files.sort()
print(f"{len(files)} parquet shards", file=sys.stderr)
out=open("/data/wiki.tsv","w",encoding="utf-8")
n=0
for fi,f in enumerate(files):
    p=hf_hub_download(REPO, f, repo_type="dataset", cache_dir="/data/hf")
    t=pq.read_table(p, columns=["id","title","text"])
    ids=t.column("id").to_pylist(); tis=t.column("title").to_pylist(); tx=t.column("text").to_pylist()
    for i,ti,b in zip(ids,tis,tx):
        ti=(ti or "").replace("\t"," ").replace("\n"," ")
        b=(b or "").replace("\t"," ").replace("\n"," ")
        out.write(f"{i}\t{ti}\t{b}\n"); n+=1
    print(f"shard {fi+1}/{len(files)} done, {n} rows", file=sys.stderr)
    import os; os.remove(p)
out.close(); print(f"TOTAL {n}", file=sys.stderr)

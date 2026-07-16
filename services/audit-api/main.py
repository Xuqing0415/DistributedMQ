import json
import os
from datetime import datetime
from typing import List, Optional
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel

app = FastAPI(title="DistributedMQ Audit API", version="1.0")

class AuditRecord(BaseModel):
    timestamp: int
    client_ip: str
    op: str
    topic: str
    partition: int
    offset: int
    group_id: Optional[str] = None
    msg_size: int

class AuditQueryResponse(BaseModel):
    records: List[AuditRecord]
    total: int

@app.get("/api/audit/search", response_model=AuditQueryResponse)
async def search_audit(
    topic: Optional[str] = Query(None, description="Filter by topic"),
    op: Optional[str] = Query(None, description="Filter by operation type: PRODUCE/CONSUME"),
    client_ip: Optional[str] = Query(None, description="Filter by client IP"),
    start_time: Optional[int] = Query(None, description="Start timestamp (nanoseconds)"),
    end_time: Optional[int] = Query(None, description="End timestamp (nanoseconds)"),
    limit: int = Query(100, description="Maximum number of records to return"),
    offset: int = Query(0, description="Offset for pagination")
):
    records = load_all_records()
    
    if topic:
        records = [r for r in records if r.get("topic") == topic]
    if op:
        records = [r for r in records if r.get("op") == op]
    if client_ip:
        records = [r for r in records if r.get("client_ip") == client_ip]
    if start_time:
        records = [r for r in records if r.get("timestamp", 0) >= start_time]
    if end_time:
        records = [r for r in records if r.get("timestamp", 0) <= end_time]
    
    total = len(records)
    records = records[offset:offset+limit]
    
    return {"records": records, "total": total}

@app.get("/api/audit/export")
async def export_audit(
    client_ip: Optional[str] = Query(None, description="Filter by client IP"),
    topic: Optional[str] = Query(None, description="Filter by topic")
):
    records = load_all_records()
    
    if client_ip:
        records = [r for r in records if r.get("client_ip") == client_ip]
    if topic:
        records = [r for r in records if r.get("topic") == topic]
    
    csv_content = "timestamp,client_ip,operation,topic,partition,offset,group_id,msg_size\n"
    for record in records:
        timestamp = datetime.fromtimestamp(record.get("timestamp", 0) / 1e9).isoformat()
        csv_content += f'{timestamp},{record.get("client_ip")},{record.get("op")},{record.get("topic")},{record.get("partition")},{record.get("offset")},{record.get("group_id", "")},{record.get("msg_size")}\n'
    
    export_dir = "./exports"
    os.makedirs(export_dir, exist_ok=True)
    filename = f"audit_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    filepath = os.path.join(export_dir, filename)
    
    with open(filepath, "w") as f:
        f.write(csv_content)
    
    return FileResponse(filepath, filename=filename, media_type="text/csv")

@app.get("/api/audit/stats")
async def get_audit_stats(
    topic: Optional[str] = Query(None, description="Filter by topic")
):
    records = load_all_records()
    
    if topic:
        records = [r for r in records if r.get("topic") == topic]
    
    produce_count = sum(1 for r in records if r.get("op") == "PRODUCE")
    consume_count = sum(1 for r in records if r.get("op") == "CONSUME")
    total_size = sum(r.get("msg_size", 0) for r in records)
    
    ip_counts = {}
    for r in records:
        ip = r.get("client_ip")
        ip_counts[ip] = ip_counts.get(ip, 0) + 1
    
    topic_counts = {}
    for r in records:
        t = r.get("topic")
        topic_counts[t] = topic_counts.get(t, 0) + 1
    
    return {
        "total_records": len(records),
        "produce_count": produce_count,
        "consume_count": consume_count,
        "total_size_bytes": total_size,
        "ip_distribution": ip_counts,
        "topic_distribution": topic_counts
    }

def load_all_records() -> List[dict]:
    log_dirs = [
        "../cluster/go/data/broker1/audit",
        "../cluster/go/data/broker2/audit", 
        "../cluster/go/data/broker3/audit",
        "./logs"
    ]
    
    all_records = []
    for log_dir in log_dirs:
        if not os.path.exists(log_dir):
            continue
        for filename in os.listdir(log_dir):
            if not filename.endswith(".log"):
                continue
            filepath = os.path.join(log_dir, filename)
            try:
                with open(filepath, "r") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                record = json.loads(line)
                                all_records.append(record)
                            except json.JSONDecodeError:
                                continue
            except Exception:
                continue
    
    all_records.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
    return all_records

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
#!/usr/bin/env python3
"""
Convert X-CANIDS parquet to STM32 C header
"""

import pandas as pd
import pyarrow.parquet as pq
import re
import os

# ============================================================================
# EDIT THESE VALUES
# ============================================================================
#parquet_path = r"C:\Users\User\Desktop\OneDrive - Universita degli Studi Roma Tre\Desktop\MATERIALE TESI POLIMI\Filtered Papers\To Study In Depth\Possible Pipeline\First Layer\New_strategyV2\New_strategy\X-CANIDS\thresholds_test\dump6-masq-080h.parquet"
parquet_path = r"C:\Users\User\Desktop\OneDrive - Universita degli Studi Roma Tre\Desktop\MATERIALE TESI POLIMI\Filtered Papers\To Study In Depth\Possible Pipeline\First Layer\New_strategyV2\New_strategy\X-CANIDS\thresholds_test\dump6-fuzz-200.parquet"
max_messages = 500
start_row = 1500000
output_file = "can_traffic.h"
# ============================================================================

print("="*60)
print("CONVERTING X-CANIDS PARQUET TO C HEADER")
print("="*60)
print(f"Input:  {parquet_path}")
print(f"Output: {output_file}")
print(f"Start:  Row {start_row:,}")
print(f"Limit:  {max_messages} messages")
print("="*60)

def parse_hex_data(data_bytes):
    """Parse data bytes from parquet"""
    if isinstance(data_bytes, bytes):
        return list(data_bytes)
    elif isinstance(data_bytes, str):
        # Handle b'\x00\x01...' format
        if data_bytes.startswith("b'") or data_bytes.startswith('b"'):
            data_bytes = data_bytes[2:-1]
        # Parse hex escapes
        result = []
        i = 0
        while i < len(data_bytes):
            if data_bytes[i:i+2] == '\\x':
                result.append(int(data_bytes[i+2:i+4], 16))
                i += 4
            else:
                result.append(ord(data_bytes[i]))
                i += 1
        return result
    else:
        return [0] * 8

if not os.path.exists(parquet_path):
    print(f"\n❌ ERROR: File not found!")
    exit(1)

print(f"\n✅ File exists")
print(f"⏳ Reading parquet...")

try:
    # Read parquet file
    table = pq.read_table(parquet_path)
    df = table.to_pandas()
    
    # Extract requested rows
    if start_row > 0:
        df = df.iloc[start_row:start_row + max_messages].copy()
    else:
        df = df.head(max_messages).copy()
    
    # Ensure timestamp is a column (not index)
    if "timestamp" not in df.columns:
        if df.index.name == "timestamp":
            df = df.reset_index()
        else:
            df = df.reset_index().rename(columns={"index": "timestamp"})
    
    # Sort by timestamp
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    print(f"✅ Loaded {len(df)} rows")
    
    # DEBUG: Show timestamp format
    print(f"\nDEBUG: First 3 timestamps:")
    for i in range(min(3, len(df))):
        ts = df.iloc[i]['timestamp']
        print(f"  Row {i}: {ts} (type: {type(ts).__name__})")
        if hasattr(ts, 'total_seconds'):
            print(f"    → {ts.total_seconds():.6f} seconds")
    
except Exception as e:
    print(f"❌ ERROR reading parquet: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

print(f"\n⏳ Writing C header...")

messages_written = 0
first_timestamp = None
attack_count = 0

with open(output_file, 'w') as f:
    f.write("// Auto-generated from X-CANIDS parquet\n")
    f.write(f"// Start row: {start_row:,}\n\n")
    f.write("#ifndef CAN_TRAFFIC_H\n")
    f.write("#define CAN_TRAFFIC_H\n\n")
    f.write("#include <stdint.h>\n\n")
    
    f.write("struct __attribute__((packed)) CANMessage {\n")
    f.write("    uint32_t timestamp_ms;\n")
    f.write("    uint16_t can_id;\n")
    f.write("    uint8_t dlc;\n")
    f.write("    uint8_t data[8];\n")
    f.write("    uint8_t label;\n")
    f.write("};\n\n")
    
    f.write("const struct CANMessage CAN_TRAFFIC[] PROGMEM = {\n")
    
    for idx, row in df.iterrows():
        can_id = int(row['arbitration_id'])
        dlc = int(row['dlc'])
        label = int(row['label']) if 'label' in row and pd.notna(row['label']) else 0
        
        # Handle timestamp (could be timedelta or float)
        ts = row['timestamp']
        if hasattr(ts, 'total_seconds'):
            timestamp_sec = ts.total_seconds()
        else:
            timestamp_sec = float(ts)
        
        if first_timestamp is None:
            first_timestamp = timestamp_sec
            rel_time_ms = 0
            print(f"\nDEBUG: First timestamp = {timestamp_sec:.6f} seconds")
        else:
            delta_sec = timestamp_sec - first_timestamp
            rel_time_ms = int(delta_sec * 1000)
            
            # DEBUG: Print first few
            if messages_written < 5:
                print(f"DEBUG: Row {messages_written}: ts={timestamp_sec:.6f}s, delta={delta_sec:.6f}s, ms={rel_time_ms}")
        
        data_bytes = parse_hex_data(row['data'])
        # Pad to 8 bytes
        while len(data_bytes) < 8:
            data_bytes.append(0)
        data_bytes = data_bytes[:8]
        
        f.write(f"    {{{rel_time_ms}, 0x{can_id:03X}, {dlc}, " +
               f"{{{', '.join([f'0x{b:02X}' for b in data_bytes])}}}, {label}}},\n")
        
        messages_written += 1
        if label == 1:
            attack_count += 1

    f.write("};\n\n")
    f.write(f"#define NUM_CAN_MESSAGES {messages_written}\n\n")
    f.write("#endif\n")

print(f"\n✅ DONE!")
print("\n" + "="*60)
print("RESULTS")
print("="*60)
print(f"📁 File created: {os.path.abspath(output_file)}")
print(f"📊 Total messages: {messages_written}")
print(f"🔴 Attack messages: {attack_count}")
print(f"🟢 Benign messages: {messages_written - attack_count}")
print(f"📦 Flash needed: ~{messages_written * 20} bytes")
print("="*60)
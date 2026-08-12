table: TBL
business_key: [id]
schema:
  - {name: id,   type: string}
  - {name: name, type: string}
  - {name: seg,  type: string}
  - {name: amt,  type: double}
  - {name: eff,  type: timestamp}
tracked_columns: [name, seg, amt]
valid_time: {mode: source_column, column: eff}
late_arrival: {policy: split}

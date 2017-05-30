
package com.pingcap.tikv.operation;


import com.pingcap.tidb.tipb.SelectRequest;
import com.pingcap.tikv.util.TiFluentIterable;
import com.pingcap.tikv.type.FieldType;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.type.StringType;
import com.pingcap.tikv.type.DecimalType;

public class TypeInferer {
    public static FieldType[] toFieldTypes(SelectRequest req) {
        if (req.getAggregatesCount() == 0) {
            return TiFluentIterable.from(req.getTableInfo().getColumnsList())
                .transform(column -> new TiColumnInfo.InternalTypeHolder(column).toFieldType())
                .toArray(FieldType.class);
        } else {
            // return TiFluentIterable.from(req.getTableInfo().getColumnsList())
            //     .transform(column -> new TiColumnInfo.InternalTypeHolder(column).toFieldType())
            //     .toArray(FieldType.class);
            // TODO: add more aggregates type
            FieldType[] fts = new FieldType[2];
            fts[0] = new StringType();
            fts[1] = new DecimalType();
            return fts;
        }
    }
}

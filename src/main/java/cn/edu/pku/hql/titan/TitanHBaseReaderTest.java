package cn.edu.pku.hql.titan;

import cn.edu.pku.hql.titan.mapreduce.SnapshotCounter;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntryList;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.idmanagement.IDInspector;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.NavigableMap;

/**
 * Test to read titan vertices and edges directly from HBase table.
 *
 * Created by huangql on 11/19/16.
 */
public class TitanHBaseReaderTest {

    private static final String TABLE_NAME_PREFIX = "storage.hbase.table=";
    private static final int TABLE_NAME_INDEX = TABLE_NAME_PREFIX.length();
    private static String getTableName(String titanConf) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(titanConf));
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.startsWith(TABLE_NAME_PREFIX)) {
                return line.substring(TABLE_NAME_INDEX);
            }
        }
        throw new IllegalArgumentException("HBase table name not found in titan conf");
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Args: titanConf vid");
            System.exit(1);
        }
        String titanConf = args[0];
        long vid = Long.parseLong(args[1]);

        StandardTitanGraph graph = (StandardTitanGraph) TitanFactory.open(titanConf);
        TitanVertex v = graph.getVertex(vid);
        if (v == null) {
            System.err.println("Vertex with id = " + vid + " not found!");
            System.exit(1);
        }

        System.out.println("--------------- Query from Titan ---------------");
        System.out.println("=============> " + v.getPropertyCount() + " properties:");
        for (TitanProperty p : v.getProperties()) {
            System.out.println(p.toString());
        }
        System.out.println("=============> " + v.getEdgeCount() + " edges:");
        for (TitanEdge e : v.getEdges()) {
            System.out.println(e.toString());
        }

        System.out.println("--------------- Query from HBase ---------------");
        HConnection conn = HConnectionManager.createConnection(HBaseConfiguration.create());
        String tableName = getTableName(titanConf);
        System.out.println("Table name: " + tableName);

        HTableInterface table = conn.getTable(tableName);

        StaticBuffer sb = graph.getIDManager().getKey(vid);
        byte[] rowKey = sb.getBytes(0, sb.length());
        Get request = new Get(rowKey);
        Result result = table.get(request);
        NavigableMap<byte[], byte[]> m = result.getFamilyMap(Bytes.toBytes("e"));
        System.out.println("Map size: " + m.size());
//        System.out.println("col\tvalue");
//        for (byte[] key : m.keySet()) {
//            System.out.println(new String(key) + "\t" + new String(m.get(key)));
//        }
        SnapshotCounter.HBaseGetter entryGetter = new SnapshotCounter.HBaseGetter();
        EntryList entryList = StaticArrayEntryList.ofBytes(
                result.getMap().get(Bytes.toBytes("e")).entrySet(),
                entryGetter);
        StandardTitanTx tx = (StandardTitanTx) graph.newTransaction();
        System.out.println("Entry list size: " + entryList.size());
        int cnt = 0;
        IDInspector inspector = graph.getIDInspector();
        for (Entry entry : entryList) {
            RelationCache relation = graph.getEdgeSerializer().readRelation(entry, false, tx);
            RelationType type = tx.getExistingRelationType(relation.typeId);
            if (type.isEdgeLabel() && !inspector.isEdgeLabelId(relation.relationId)) {
                cnt++;
//                System.out.print("isSystemRelationTypeId: ");
//                System.out.println(inspector.isSystemRelationTypeId(relation.typeId));
//
//                System.out.print("isEdgeLabelId: ");
//                System.out.println(inspector.isEdgeLabelId(relation.typeId));
//
//                System.out.print("relationId isEdgeLabelId: ");
//                System.out.println(inspector.isEdgeLabelId(relation.relationId));
//
//                System.out.print("relationId isSystemRelationTypeId: ");
//                System.out.println(inspector.isSystemRelationTypeId(relation.relationId));
            }
        }
        System.out.println("Edge count: " + cnt);
        graph.shutdown();
    }
}

// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tools;

import net.opentsdb.core.FederatedMetricIndex;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.model.Change;
import net.opentsdb.core.model.FederatedMetric;
import net.opentsdb.core.model.SubMetric;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import org.hbase.async.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.*;

/**
 * Command line tool to manipulate UIDs.
 * Can be used to find or assign UIDs.
 */
final class AddIndex {
    public static void main(String[] args) throws Exception {
        ArgP argp = new ArgP();
        CliOptions.addCommon(argp);
        CliOptions.addVerbose(argp);
        argp.addOption("--idwidth", "N",
                "Number of bytes on which the UniqueId is encoded.");
        argp.addOption("--ignore-case",
                "Ignore case distinctions when matching a regexp.");
        argp.addOption("-i", "Short for --ignore-case.");
        args = CliOptions.parse(argp, args);

        final String table = argp.get("--table", "tsdb");
        final String uidtable = argp.get("--uidtable", "tsdb-uid");
        final String indextable = argp.get("--indextable", "tsdb-index");
        final short idwidth = (argp.has("--idwidth")
                                   ? Short.parseShort(argp.get("--idwidth"))
                                   : 3);

        final HBaseClient client = CliOptions.clientFromOptions(argp);

        TSDB tsdb = new TSDB(client, table, uidtable, indextable);
        FederatedMetricIndex index = FederatedMetricIndex.load(tsdb, indextable.getBytes());

        try {
            if (args[0].equals("add")) {
                if (args.length<3) {
                    throw new RuntimeException("You should specify tags, like: index add metric key=value[ key=value[..]]");
                }

                Change change = new Change();
                change.tags = new HashMap<String, String>();
                change.ts = System.currentTimeMillis();
                for(int i=2;i<args.length;i++) {
                    String[] parts = args[i].split("=");
                    if (parts.length!=2) throw new RuntimeException("Got: " + args[i] + " but should be: key=value");
                    if (change.tags.containsKey(parts[0])) {
                        throw  new RuntimeException("Collision in keys: " + parts[0]);
                    }
                    change.tags.put(parts[0], parts[1]);
                }

                SortedSet<Change> changes = index.get(args[1]);
                changes = changes==null ? new TreeSet<Change>() : new TreeSet<Change>(changes);
                changes.add(change);

                // check change is consistent
                FederatedMetric metric = FederatedMetric.create(args[1], changes);

                final UniqueId uid = new UniqueId(client, uidtable.getBytes(), "metrics", (int) idwidth);
                uid.getOrCreateId(change.subMetricName(metric.metric));

                index.put(metric.metric, changes);
            } else if (args[0].equals("list")) {
                for(FederatedMetric metric : index.list()) {
                    System.out.println("Federated metric: " + metric.metric);
                    for (SubMetric subMetric : metric.subMetrics) {
                        System.out.println("\tsubmetric: " + subMetric.name);
                        for (String key : subMetric.tags.keySet()) {
                            System.out.println("\t\t" + key + "=" + subMetric.tags.get(key));
                        }
                    }
                }
            } else {
                throw new RuntimeException("Unknown command: " + args[0]);
            }
        } finally {
            client.shutdown().joinUninterruptibly();
        }
    }
}

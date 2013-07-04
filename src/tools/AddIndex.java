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

import net.opentsdb.core.index.HBaseIndex;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.index.Index;
import net.opentsdb.core.index.model.Change;
import net.opentsdb.core.index.model.FederatedMetric;
import net.opentsdb.core.index.model.SubMetric;
import net.opentsdb.uid.UniqueId;
import org.hbase.async.*;

import java.util.*;

/**
 * Command line tool to manipulate UIDs.
 * Can be used to find or assign UIDs.
 */
final class AddIndex {
    final HBaseClient client;
    final TSDB tsdb;
    final Index index;
    final String[] args;
    final String uidtable;

    private AddIndex(String[] args) {
        ArgP argp = new ArgP();
        CliOptions.addCommon(argp);
        CliOptions.addVerbose(argp);
        argp.addOption("--idwidth", "N",
                "Number of bytes on which the UniqueId is encoded.");
        argp.addOption("--ignore-case",
                "Ignore case distinctions when matching a regexp.");
        argp.addOption("-i", "Short for --ignore-case.");
        this.args = CliOptions.parse(argp, args);

        final String table = argp.get("--table", "tsdb");
        uidtable = argp.get("--uidtable", "tsdb-uid");
        final String indextable = argp.get("--indextable", "tsdb-index");
        // TODO: change 1000 to 60*10*1000
        final Long cacheTimeoutMs = Long.parseLong(argp.get("--cache-timeout-ms", "1000"));
        client = CliOptions.clientFromOptions(argp);
        tsdb = new TSDB(client, table, uidtable, indextable, cacheTimeoutMs);
        index = new HBaseIndex.Loader(tsdb.asIdResolver(), client, indextable.getBytes()).load();
    }

    private void addOrRemove() {
        if (args.length<3) {
            throw new RuntimeException("You should specify tags, like: index add/remove metric key=value[ key=value[..]]");
        }

        HashMap<String, String> tags = new HashMap<String, String>();
        for(int i=2;i<args.length;i++) {
            String[] parts = args[i].split("=");
            if (parts.length!=2) throw new RuntimeException("Got: " + args[i] + " but should be: key=value");
            if (tags.containsKey(parts[0])) {
                throw  new RuntimeException("Collision in keys: " + parts[0]);
            }
            tags.put(parts[0], parts[1]);
        }

        if (args[0].equals("add")) {
            index.addIndex(args[1], tags);
        } else if (args[0].equals("remove")) {
            index.removeIndex(args[1], tags);
        } else {
            throw new RuntimeException();
        }
    }

    private void list() {
        for(FederatedMetric metric : index.list()) {
            System.out.println("Federated metric: " + metric.metric);
            for (SubMetric subMetric : metric.subMetrics) {
                System.out.println("\tsubmetric: " + subMetric.name);
                for (String key : subMetric.tags.keySet()) {
                    System.out.println("\t\t" + key + "=" + subMetric.tags.get(key));
                }
            }
        }
    }


    private void shutdown() throws Exception {
        client.shutdown().joinUninterruptibly();
    }

    public static void main(String[] args) throws Exception {
        AddIndex index = new AddIndex(args);
        try {

            if (index.args[0].equals("add")) {
                index.addOrRemove();
            } else if (args[0].equals("remove")) {
                index.addOrRemove();
            } else if (index.args[0].equals("list")) {
                index.list();
            } else {
                throw new RuntimeException("Unknown command: " + args[0]);
            }
        } finally {
            index.shutdown();
        }
    }

    //private SortedSet<Change>
}

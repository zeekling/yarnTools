package org.hadoop.yarn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class NodeManagerLevelDBTools {

    public static final Logger LOG = LoggerFactory.getLogger(NodeManagerLevelDBTools.class);

    public static void main(String[] args) {
        if (args.length != 1) {
            throw new RuntimeException("Usage: NodeManagerLevelDBTools <levelDbPath>");
        }
        String levelDbPath = args[0];
        LevelDBStateStore stateStore = new LevelDBStateStore(levelDbPath);

    }

}

package org.hadoop.yarn;

import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;
import org.hadoop.yarn.state.RecoveredContainerState;
import org.hadoop.yarn.state.RecoveredContainerStatus;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class NodeManagerLevelDBTools {

    private static final Options cliOptions = initOptions();

    private static Options initOptions() {
        Options cliOptions = new Options();
        StateStoreOptions[] values = StateStoreOptions.values();
        for (StateStoreOptions value : values) {
            cliOptions.addOption(value.getOption());
        }
        return cliOptions;
    }

    private static NMLevelDbOptions initNMLevelDbOptions(String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(cliOptions, args);
        NMLevelDbOptions.NMLevelDbOptionsBuilder builder = NMLevelDbOptions.builder();
        builder.levelDbPath(commandLine.getOptionValue(StateStoreOptions.LEVELDB_PATH.getOption()));
        builder.isContainer(commandLine.hasOption(StateStoreOptions.CONTAINER.getOption()));
        return builder.build();
    }

    private static void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("NodeManagerLevelDBTools OPTIONS [leveldb_path...] \n\nOPTIONS", cliOptions);
    }

    private static void containerSummary(NMLevelDbOptions nmLevelDbOptions, LevelDBStateStore stateStore) {
        List<RecoveredContainerState> allContainers = stateStore.getAllContainers();
        Map<RecoveredContainerStatus, Integer> allContainersStatus = new HashMap<>();

        for (RecoveredContainerState recoveredContainerState : allContainers) {
            RecoveredContainerStatus status = recoveredContainerState.getStatus();
            allContainersStatus.put(status, allContainersStatus.getOrDefault(status, 0) + 1);
        }
        System.out.println("=====================Containers Summary==============");
        System.out.println("LevelDb path "+ nmLevelDbOptions.getLevelDbPath());
        System.out.println("all containers " + allContainers.size());
        for (Map.Entry<RecoveredContainerStatus, Integer> entry : allContainersStatus.entrySet()) {
            System.out.println("container status " + entry.getKey() + " has " + entry.getValue() + " containers");
        }
        System.out.println("======================end============================");
    }

    public static void main(String[] args) throws ParseException {

        NMLevelDbOptions nmLevelDbOptions = initNMLevelDbOptions(args);
        if (StringUtils.isEmpty(nmLevelDbOptions.getLevelDbPath())) {
            printHelp();
            return;
        }
        String levelDbPath = nmLevelDbOptions.getLevelDbPath();
        LevelDBStateStore stateStore = new LevelDBStateStore(levelDbPath);
        if (nmLevelDbOptions.isContainer()) {
            containerSummary(nmLevelDbOptions, stateStore);
            return;
        }
    }

}

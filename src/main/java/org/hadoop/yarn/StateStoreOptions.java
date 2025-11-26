package org.hadoop.yarn;
import lombok.Getter;
import org.apache.commons.cli.Option;

@Getter
public enum StateStoreOptions {

    LEVELDB_PATH(new Option("leveldb_path", true, "level path")),

    CONTAINER(new Option("container", false, "container info")),;

    private final Option option;

    StateStoreOptions(Option option) {
        this.option = option;
    }

}

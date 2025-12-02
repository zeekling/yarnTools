package org.hadoop.yarn;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;

@Builder
@Getter
public class NMLevelDbOptions {

    private String levelDbPath;

    private boolean isContainer = false;

    private boolean showDetail = false;
}

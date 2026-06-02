package com.github.chengyuxing.sql.terminal.cli;

import com.github.chengyuxing.common.console.Style;
import com.github.chengyuxing.sql.terminal.cli.component.Prompt;
import com.github.chengyuxing.sql.terminal.types.View;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public abstract class Context {
    /**
     * transaction state
     */
    public static final AtomicBoolean txActive = new AtomicBoolean(false);
    /**
     * display and output format state
     */
    public static final AtomicReference<View> viewMode = new AtomicReference<>(View.tsv);
    /**
     * Cli prompt ref
     */
    public static final AtomicReference<Prompt> promptReference = new AtomicReference<>(new Prompt(""));
    /**
     * query result output path
     */
    public static final AtomicReference<String> outputPath = new AtomicReference<>("");
    /**
     * is cli in sql appending state
     */
    public static final AtomicBoolean appending = new AtomicBoolean(false);

    /**
     * Get the cli prompt state text currently.
     *
     * @param defaults defaults text
     * @return state text
     */
    public static String getPromptState(String defaults) {
        if (promptReference.get() != null) {
            Prompt prompt = promptReference.get();

            // update transaction state color
            prompt.setStyle(txActive.get() ? Style.YELLOW : Style.PURPLE);

            // multi sql appending
            if (appending.get()) {
                prompt.append();
                return prompt.getValue();
            }

            // update output redirect mode
            String path = outputPath.get();
            if (path.isEmpty()) {
                prompt.newLine();
            } else {
                String statusText = path.length() > 30 ? path.substring(0, 10) + "..." + path.substring(path.length() - 10) : path;
                statusText = statusText + "< ";
                prompt.custom(statusText);
            }
            return prompt.getValue();
        }
        return defaults;
    }
}

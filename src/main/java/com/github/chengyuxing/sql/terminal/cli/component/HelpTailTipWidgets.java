package com.github.chengyuxing.sql.terminal.cli.component;

import org.jline.keymap.KeyMap;
import org.jline.reader.LineReader;
import org.jline.reader.Reference;
import org.jline.widget.TailTipWidgets;
import org.jline.widget.Widgets;

public class HelpTailTipWidgets extends Widgets {
    private int maxWindowSize;
    private final TailTipWidgets tailTipWidgets;

    public HelpTailTipWidgets(LineReader reader, int maxWindowSize, TailTipWidgets tailTipWidgets) {
        super(reader);
        this.maxWindowSize = maxWindowSize;
        this.tailTipWidgets = tailTipWidgets;
        addWidget("change-window-size", this::changeWindowSize);
    }

    public boolean changeWindowSize() {
        if (tailTipWidgets.getDescriptionSize() == 0) {
            tailTipWidgets.setDescriptionSize(maxWindowSize);
            tailTipWidgets.toggleWindow();
        }
        tailTipWidgets.toggleWindow();
        return true;
    }

    public void setMaxWindowSize(int maxWindowSize) {
        this.maxWindowSize = maxWindowSize;
    }
}

import plotly.plotly as py
# Put your username and password here in order to draw with plot.ly
py.sign_in(USERNAME, PASSWORD)

from plotly.graph_objs import Scatter, Bar, Figure, Layout
from plotly.offline import plot
import pandas as pd

# Draw scatter with one or two y axis.
#
# Input:
#   lines: It is the array for lines. Each line comprise two elements for x and y.
#   notes: Its type is dict.
#       {'colors', 'names', 'modes', 'title', 'title_size',
#        'x_title', 'x_title_size', 'x_tick_size', 'x_dtick', 'x_linewidth',
#        'x2_title', 'x2_title_size', 'x2_tick_size', 'x2_dtick', 'x2_linewidth', 'x2_overlaxing', 'x2_side', 'x2_zeroline',
#        'y_title', 'y_title_size', 'y_tick_size', 'y_dtick', 'y_linewidth', 'y_tickvals', 'y_ticktext', 'y_zeroline',
#        'y2_title', 'y2_title_size', 'y2_tick_size', 'y2_dtick', 'y2_linewidth', 'y2_overlaying', 'y2_side', 'y2_zeroline', 'y2_tickvals', 'y2_ticktext',
#        'y3_title', 'y3_title_size', 'y3_tick_size', 'y3_dtick', 'y3_linewidth', 'y3_overlaying', 'y3_side', 'y3_zeroline', 'y3_tickvals', 'y3_ticktext',
#        'margin_l', 'margin_r', 'margin_b', 'margin_t', 'margin_pad',
#        'legend_x', 'legend_y', 'legend_size', 'legend_orientation'
#        'height', 'width', 'filename'}
# Output:
#   Graph with lines+markers or lines or markers
#
# Notes:
#   1. 'overlaying': Show edges of the graph Wrong: decide which axis is on which axis
#   2. y2 should be after y

def draw_scatter(lines, notes):
    colors = notes['colors']
    names = notes['names']
    modes = notes['modes']
    xaxises = notes['xaxises']
    yaxises = notes['yaxises']
    x_dtick = notes['x_dtick']
    y_dtick = notes['y_dtick']
    filename = notes['filename']

    two_xaxises = False
    pre_xaxis = ''
    for _xaxis in xaxises:
        if len(pre_xaxis) == 0:
            pre_xaxis = _xaxis
        elif pre_xaxis != _xaxis:
            two_xaxises = True
            break

    y_showticklabels = False
    if 'y_tickvals' in notes and 'y_ticktext' in notes:
        if len(notes['y_tickvals']) > 0 and len(notes['y_ticktext']) > 0:
            y_showticklabels = True
    y2_showticklabels = False
    if 'y2_tickvals' in notes and 'y2_ticktext' in notes:
        if len(notes['y2_tickvals']) > 0 and len(notes['y2_ticktext']) > 0:
            y2_showticklabels = True

    traces = []
    for i in range(0, len(lines)):
        if 'lines' in modes[i]:
            traces.append(Scatter(x = lines[i][0], y = lines[i][1], xaxis = xaxises[i], yaxis = yaxises[i], name = names[i], mode = modes[i], line = dict(color = colors[i])))
        else:
            traces.append(Scatter(x = lines[i][0], y = lines[i][1], xaxis = xaxises[i], yaxis = yaxises[i], name = names[i], mode = modes[i], marker = dict(color = colors[i])))

    layout = Layout(
        title = '' if 'title' not in notes else notes['title'],
        titlefont = dict(
            size = 36 if 'title_size' not in notes else notes['title_size'],
        ),
        xaxis = dict(
            title = '' if 'x_title' not in notes else notes['x_title'],
            titlefont = dict(
                size = 32 if 'x_title_size' not in notes else notes['x_title_size'],
            ),
            tickfont = dict(
                size = 26 if 'x_tick_size' not in notes else notes['x_tick_size'],
            ),
            linewidth = 2 if 'x_linewidth' not in notes else notes['x_linewidth'],
            dtick = x_dtick,
            mirror = True,
            domain = [0.0, 1.0] if 'y3' in notes['yaxises'] or 'y4' in notes['yaxises'] else [0.0, 0.80],
        ),
        xaxis2 = dict(
            title = '' if 'x2_title' not in notes else notes['x2_title'],
            titlefont = dict(
                size = 32 if 'x2_title_size' not in notes else notes['x2_title_size'],
            ),
            tickfont = dict(
                size = 26 if 'x2_tick_size' not in notes else notes['x2_tick_size'],
            ),
            linewidth = 2 if 'x2_linewidth' not in notes else notes['x2_linewidth'],
            overlaying = 'x' if 'x2_overlaying' not in notes else notes['x2_overlaying'],
            zeroline = True if 'x2_zeroline' not in notes else notes['x2_zeroline'],
            side = 'top' if 'x2_side' not in notes else notes['x2_side'],
            visible = False if not two_xaxises else True,
        ),
        yaxis = dict(
            title = '' if 'y_title' not in notes else notes['y_title'],
            titlefont = dict(
                size = 32 if 'y_title_size' not in notes else notes['y_title_size'],
            ),
            tickfont = dict(
                size = 26 if 'y_tick_size' not in notes else notes['y_tick_size'],
            ),
            dtick = y_dtick,
            linewidth = 2 if 'y_linewidth' not in notes else notes['y_linewidth'],
            zeroline = True if 'y_zeroline' not in notes else notes['y_zeroline'],
            #overlaying = 'y2',
            mirror = True,
            #showticklabels = y_showticklabels,
            #tickvals = [] if 'y_tickvals' not in notes else notes['y_tickvals'],
            #ticktext = [] if 'y_ticktext' not in notes else notes['y_ticktext'],
        ),
        yaxis2 = dict(
            title = '' if 'y2_title' not in notes else notes['y2_title'],
            titlefont = dict(
                size = 32 if 'y2_title_size' not in notes else notes['y2_title_size'],
                color = 'black' if 'y2_title_color' not in notes else notes['y2_title_color'],
            ),
            tickfont = dict(
                size = 26 if 'y2_tick_size' not in notes else notes['y2_tick_size'],
                color = 'black' if 'y2_tick_color' not in notes else notes['y2_tick_color'],
            ),
            dtick = y_dtick if 'y2_dtick' not in notes else notes['y2_dtick'],
            linewidth = 2 if 'y2_linewidth' not in notes else notes['y2_linewidth'],
            overlaying = 'y' if 'y2_overlaying' not in notes else notes['y2_overlaying'],
            zeroline = True if 'y2_zeroline' not in notes else notes['y2_zeroline'],
            side = 'right' if 'y2_side' not in notes else notes['y2_side'],
            anchor = 'x',
            visible = False if 'y2' not in notes['yaxises'] else True,
            #showticklabels = y2_showticklabels,
            #tickvals = [] if 'y2_tickvals' not in notes else notes['y2_tickvals'],
            #ticktext = [] if 'y2_ticktext' not in notes else notes['y2_ticktext'],
        ),
        yaxis3 = dict(
            title = '' if 'y3_title' not in notes else notes['y3_title'],
            titlefont = dict(
                size = 32 if 'y3_title_size' not in notes else notes['y3_title_size'],
                color = 'black' if 'y3_title_color' not in notes else notes['y3_title_color'],
            ),
            tickfont = dict(
                size = 26 if 'y3_tick_size' not in notes else notes['y3_tick_size'],
                color = 'black' if 'y3_tick_color' not in notes else notes['y3_tick_color'],
            ),
            dtick = y_dtick if 'y3_dtick' not in notes else notes['y3_dtick'],
            linewidth = 0 if 'y3_linewidth' not in notes else notes['y3_linewidth'],
            overlaying = 'y' if 'y3_overlaying' not in notes else notes['y3_overlaying'],
            zeroline = True if 'y3_zeroline' not in notes else notes['y3_zeroline'],
            side = 'right' if 'y3_side' not in notes else notes['y3_side'],
            anchor = 'free',
            position = 0.900,
            visible = False if 'y3' not in notes['yaxises'] else True,
            #showticklabels = y3_showticklabels,
            #tickvals = [] if 'y3_tickvals' not in notes else notes['y3_tickvals'],
            #ticktext = [] if 'y3_ticktext' not in notes else notes['y3_ticktext'],
        ),
        yaxis4 = dict(
            title = '' if 'y4_title' not in notes else notes['y4_title'],
            titlefont = dict(
                size = 32 if 'y4_title_size' not in notes else notes['y4_title_size'],
                color = 'black' if 'y4_title_color' not in notes else notes['y4_title_color'],
            ),
            tickfont = dict(
                size = 26 if 'y4_tick_size' not in notes else notes['y4_tick_size'],
                color = 'black' if 'y4_tick_color' not in notes else notes['y4_tick_color'],
            ),
            dtick = y_dtick if 'y4_dtick' not in notes else notes['y4_dtick'],
            linewidth = 0 if 'y4_linewidth' not in notes else notes['y4_linewidth'],
            overlaying = 'y' if 'y4_overlaying' not in notes else notes['y4_overlaying'],
            zeroline = True if 'y4_zeroline' not in notes else notes['y4_zeroline'],
            side = 'right' if 'y4_side' not in notes else notes['y4_side'],
            anchor = 'free',
            position = 0.995,
            visible = False if 'y4' not in notes['yaxises'] else True,
            #showticklabels = y4_showticklabels,
            #tickvals = [] if 'y4_tickvals' not in notes else notes['y4_tickvals'],
            #ticktext = [] if 'y4_ticktext' not in notes else notes['y4_ticktext'],
        ),
        margin = dict(
            l = 130 if 'margin_l' not in notes else notes['margin_l'],
            r = 100 if 'margin_r' not in notes else notes['margin_r'],
            b = 80 if 'margin_b' not in notes else notes['margin_b'],
            t = 120 if 'margin_t' not in notes else notes['margin_t'],
            pad = 4 if 'margin_pad' not in notes else notes['margin_pad'],
        ),
        legend = dict(
            x = 0 if 'legend_x' not in notes else notes['legend_x'],
            y = 1 if 'legend_y' not in notes else notes['legend_y'],
            orientation = 'v' if 'legend_orientation' not in notes else notes['legend_orientation'],
            font = dict(
                size = 24 if 'legend_size' not in notes else notes['legend_size'],
            ),
        ),
    )
    fig = Figure(data = traces, layout = layout)
    plot(fig, filename = filename + '.html')
    py.image.save_as(fig, filename = filename + '.png', 
        height = 650 if 'height' not in notes else notes['height'], 
        width = 1120 if 'width' not in notes else notes['width'])

def draw_scatter_with_ytick(lines, notes):
    colors = notes['colors']
    names = notes['names']
    modes = notes['modes']
    xaxises = notes['xaxises']
    yaxises = notes['yaxises']
    x_dtick = notes['x_dtick']
    y_dtick = notes['y_dtick']
    filename = notes['filename']

    two_xaxises = False
    pre_xaxis = ''
    for _xaxis in xaxises:
        if len(pre_xaxis) == 0:
            pre_xaxis = _xaxis
        elif pre_xaxis != _xaxis:
            two_xaxises = True
            break

    two_yaxises = False
    pre_yaxis = ''
    for _yaxis in yaxises:
        if len(pre_yaxis) == 0:
            pre_yaxis = _yaxis
        elif pre_yaxis != _yaxis:
            two_yaxises = True
            break

    y_showticklabels = False
    if 'y_tickvals' in notes and 'y_ticktext' in notes:
        if len(notes['y_tickvals']) > 0 and len(notes['y_ticktext']) > 0:
            y_showticklabels = True
    y2_showticklabels = False
    if 'y2_tickvals' in notes and 'y2_ticktext' in notes:
        if len(notes['y2_tickvals']) > 0 and len(notes['y2_ticktext']) > 0:
            y2_showticklabels = True

    traces = []
    for i in range(0, len(lines)):
        if 'lines' in modes[i]:
            traces.append(Scatter(x = lines[i][0], y = lines[i][1], xaxis = xaxises[i], yaxis = yaxises[i], name = names[i], mode = modes[i], line = dict(color = colors[i])))
        else:
            traces.append(Scatter(x = lines[i][0], y = lines[i][1], xaxis = xaxises[i], yaxis = yaxises[i], name = names[i], mode = modes[i], marker = dict(color = colors[i])))

    layout = Layout(
        title = '' if 'title' not in notes else notes['title'],
        titlefont = dict(
            size = 36 if 'title_size' not in notes else notes['title_size'],
        ),
        xaxis = dict(
            title = '' if 'x_title' not in notes else notes['x_title'],
            titlefont = dict(
                size = 32 if 'x_title_size' not in notes else notes['x_title_size'],
            ),
            tickfont = dict(
                size = 26 if 'x_tick_size' not in notes else notes['x_tick_size'],
            ),
            linewidth = 2 if 'x_linewidth' not in notes else notes['x_linewidth'],
            dtick = x_dtick,
            mirror = True,
        ),
        xaxis2 = dict(
            title = '' if 'x2_title' not in notes else notes['x2_title'],
            titlefont = dict(
                size = 32 if 'x2_title_size' not in notes else notes['x2_title_size'],
            ),
            tickfont = dict(
                size = 26 if 'x2_tick_size' not in notes else notes['x2_tick_size'],
            ),
            linewidth = 2 if 'x2_linewidth' not in notes else notes['x2_linewidth'],
            overlaying = 'x' if 'x2_overlaying' not in notes else notes['x2_overlaying'],
            zeroline = True if 'x2_zeroline' not in notes else notes['x2_zeroline'],
            side = 'top' if 'x2_side' not in notes else notes['x2_side'],
            visible = False if not two_xaxises else True,
        ),
        yaxis = dict(
            title = '' if 'y_title' not in notes else notes['y_title'],
            titlefont = dict(
                size = 32 if 'y_title_size' not in notes else notes['y_title_size'],
            ),
            tickfont = dict(
                size = 26 if 'y_tick_size' not in notes else notes['y_tick_size'],
            ),
            dtick = y_dtick,
            linewidth = 2 if 'y_linewidth' not in notes else notes['y_linewidth'],
            zeroline = True if 'y_zeroline' not in notes else notes['y_zeroline'],
            #overlaying = 'y2',
            mirror = True,
            showticklabels = y_showticklabels,
            tickvals = [] if 'y_tickvals' not in notes else notes['y_tickvals'],
            ticktext = [] if 'y_ticktext' not in notes else notes['y_ticktext'],
        ),
        yaxis2 = dict(
            title = '' if 'y2_title' not in notes else notes['y2_title'],
            titlefont = dict(
                size = 32 if 'y2_title_size' not in notes else notes['y2_title_size'],
            ),
            tickfont = dict(
                size = 26 if 'y2_tick_size' not in notes else notes['y2_tick_size'],
            ),
            dtick = y_dtick if 'y2_dtick' not in notes else notes['y2_dtick'],
            linewidth = 2 if 'y2_linewidth' not in notes else notes['y2_linewidth'],
            overlaying = 'y' if 'y2_overlaying' not in notes else notes['y2_overlaying'],
            zeroline = True if 'y2_zeroline' not in notes else notes['y2_zeroline'],
            side = 'right' if 'y2_side' not in notes else notes['y2_side'],
            visible = False if not two_yaxises else True,
            #showticklabels = y2_showticklabels,
            #tickvals = [] if 'y2_tickvals' not in notes else notes['y2_tickvals'],
            #ticktext = [] if 'y2_ticktext' not in notes else notes['y2_ticktext'],
        ),
        margin = dict(
            l = 130 if 'margin_l' not in notes else notes['margin_l'],
            r = 100 if 'margin_r' not in notes else notes['margin_r'],
            b = 80 if 'margin_b' not in notes else notes['margin_b'],
            t = 120 if 'margin_t' not in notes else notes['margin_t'],
            pad = 4 if 'margin_pad' not in notes else notes['margin_pad'],
        ),
        legend = dict(
            x = 0 if 'legend_x' not in notes else notes['legend_x'],
            y = 1 if 'legend_y' not in notes else notes['legend_y'],
            orientation = 'v' if 'legend_orientation' not in notes else notes['legend_orientation'],
            font = dict(
                size = 24 if 'legend_size' not in notes else notes['legend_size'],
            ),
        ),
    )
    fig = Figure(data = traces, layout = layout)
    plot(fig, filename = filename + '.html')
    py.image.save_as(fig, filename = filename + '.png', 
        height = 650 if 'height' not in notes else notes['height'], 
        width = 1120 if 'width' not in notes else notes['width'])

# Draw scatter with error bar.
#
# Input:
#   lines: It is the array for lines. Each line comprise two elements for x and y.
#   notes: Its type is dict.
#       {'colors', 'names', 'modes', 'title', 'title_size',
#        'x_title', 'x_title_size', 'x_tick_size', 'x_dtick', 'x_linewidth',
#        'x2_title', 'x2_title_size', 'x2_tick_size', 'x2_dtick', 'x2_linewidth', 'x2_overlaxing', 'x2_side', 'x2_zeroline',
#        'y_title', 'y_title_size', 'y_tick_size', 'y_dtick', 'y_linewidth', 'y_tickvals', 'y_ticktext',
#        'y2_title', 'y2_title_size', 'y2_tick_size', 'y2_dtick', 'y2_linewidth', 'y2_overlaying', 'y2_side', 'y2_zeroline', 'y2_tickvals', 'y2_ticktext',
#        'margin_l', 'margin_r', 'margin_b', 'margin_t', 'margin_pad',
#        'legend_x', 'legend_y', 'legend_size',
#        'height', 'width', 'filename'}
# Output:
#   Graph with lines+markers or lines or markers
#
# Notes:
#   'overlaying': decide which axis is on which axis

def draw_scatter_with_error_bar(lines, notes):
    colors = notes['colors']
    names = notes['names']
    modes = notes['modes']
    xaxises = notes['xaxises']
    yaxises = notes['yaxises']
    x_dtick = notes['x_dtick']
    y_dtick = notes['y_dtick']
    filename = notes['filename']

    two_xaxises = False
    pre_xaxis = ''
    for _xaxis in xaxises:
        if len(pre_xaxis) == 0:
            pre_xaxis = _xaxis
        elif pre_xaxis != _xaxis:
            two_xaxises = True
            break

    two_yaxises = False
    pre_yaxis = ''
    for _yaxis in yaxises:
        if len(pre_yaxis) == 0:
            pre_yaxis = _yaxis
        elif pre_yaxis != _yaxis:
            two_yaxises = True
            break

    y_showticklabels = False
    if 'y_tickvals' in notes and 'y_ticktext' in notes:
        if len(notes['y_tickvals']) > 0 and len(notes['y_ticktext']) > 0:
            y_showticklabels = True
    y2_showticklabels = False
    if 'y2_tickvals' in notes and 'y2_ticktext' in notes:
        if len(notes['y2_tickvals']) > 0 and len(notes['y2_ticktext']) > 0:
            y2_showticklabels = True

    traces = []
    for i in range(0, len(lines)):
        if 'lines' in modes[i]:
            traces.append(Scatter(x = lines[i][0], y = lines[i][1], error_y = dict(type = 'data', array = lines[i][2], visible = True), xaxis = xaxises[i], yaxis = yaxises[i], name = names[i], mode = modes[i], line = dict(color = colors[i])))
        else:
            traces.append(Scatter(x = lines[i][0], y = lines[i][1], error_y = dict(type = 'data', array = lines[i][2], visible = True), xaxis = xaxises[i], yaxis = yaxises[i], name = names[i], mode = modes[i], marker = dict(color = colors[i])))

    layout = Layout(
        title = '' if 'title' not in notes else notes['title'],
        titlefont = dict(
            size = 36 if 'title_size' not in notes else notes['title_size'],
        ),
        xaxis = dict(
            title = '' if 'x_title' not in notes else notes['x_title'],
            titlefont = dict(
                size = 32 if 'x_title_size' not in notes else notes['x_title_size'],
            ),
            tickfont = dict(
                size = 26 if 'x_tick_size' not in notes else notes['x_tick_size'],
            ),
            linewidth = 2 if 'x_linewidth' not in notes else notes['x_linewidth'],
            dtick = x_dtick,
            mirror = True,
        ),
        xaxis2 = dict(
            title = '' if 'x2_title' not in notes else notes['x2_title'],
            titlefont = dict(
                size = 32 if 'x2_title_size' not in notes else notes['x2_title_size'],
            ),
            tickfont = dict(
                size = 26 if 'x2_tick_size' not in notes else notes['x2_tick_size'],
            ),
            linewidth = 2 if 'x2_linewidth' not in notes else notes['x2_linewidth'],
            overlaying = 'x' if 'x2_overlaying' not in notes else notes['x2_overlaying'],
            zeroline = True if 'x2_zeroline' not in notes else notes['x2_zeroline'],
            side = 'top' if 'x2_side' not in notes else notes['x2_side'],
            visible = False if not two_xaxises else True,
        ),
        yaxis = dict(
            title = '' if 'y_title' not in notes else notes['y_title'],
            titlefont = dict(
                size = 32 if 'y_title_size' not in notes else notes['y_title_size'],
            ),
            tickfont = dict(
                size = 26 if 'y_tick_size' not in notes else notes['y_tick_size'],
            ),
            dtick = y_dtick,
            linewidth = 2 if 'y_linewidth' not in notes else notes['y_linewidth'],
            overlaying = 'y2',
            mirror = True,
            #showticklabels = y_showticklabels,
            #tickvals = [] if 'y_tickvals' not in notes else notes['y_tickvals'],
            #ticktext = [] if 'y_ticktext' not in notes else notes['y_ticktext'],
        ),
        yaxis2 = dict(
            title = '' if 'y2_title' not in notes else notes['y2_title'],
            titlefont = dict(
                size = 32 if 'y2_title_size' not in notes else notes['y2_title_size'],
            ),
            tickfont = dict(
                size = 26 if 'y2_tick_size' not in notes else notes['y2_tick_size'],
            ),
            dtick = y_dtick if 'y2_dtick' not in notes else notes['y2_dtick'],
            linewidth = 2 if 'y2_linewidth' not in notes else notes['y2_linewidth'],
            #overlaying = 'y' if 'y2_overlaying' not in notes else notes['y2_overlaying'],
            zeroline = True if 'y2_zeroline' not in notes else notes['y2_zeroline'],
            side = 'right' if 'y2_side' not in notes else notes['y2_side'],
            visible = False if not two_yaxises else True,
            #showticklabels = y2_showticklabels,
            #tickvals = [] if 'y2_tickvals' not in notes else notes['y2_tickvals'],
            #ticktext = [] if 'y2_ticktext' not in notes else notes['y2_ticktext'],
        ),
        margin = dict(
            l = 130 if 'margin_l' not in notes else notes['margin_l'],
            r = 100 if 'margin_r' not in notes else notes['margin_r'],
            b = 80 if 'margin_b' not in notes else notes['margin_b'],
            t = 120 if 'margin_t' not in notes else notes['margin_t'],
            pad = 4 if 'margin_pad' not in notes else notes['margin_pad'],
        ),
        legend = dict(
            x = 0 if 'legend_x' not in notes else notes['legend_x'],
            y = 1 if 'legend_y' not in notes else notes['legend_y'],
            font = dict(
                size = 24 if 'legend_size' not in notes else notes['legend_size'],
            ),
        ),
    )
    fig = Figure(data = traces, layout = layout)
    plot(fig, filename = filename + '.html')
    py.image.save_as(fig, filename = filename + '.png', 
        height = 650 if 'height' not in notes else notes['height'], 
        width = 1120 if 'width' not in notes else notes['width'])

#
# barmode: stack, group
#
def draw_bar_and_scatter(data, notes):
    filename = notes['filename']
    bar_colors = notes['bar_colors']
    bar_names = notes['bar_names']
    bar_mode = notes['bar_mode']
    x_dtick = 1 if 'x_dtick' not in notes else notes['x_dtick']
    y_dtick = 1 if 'y_dtick' not in notes else notes['y_dtick']

    line_colors = [] if 'line_colors' not in notes else notes['line_colors']
    modes = [] if 'modes' not in notes else notes['modes']
    xaxises = [] if 'xaxises' not in notes else notes['xaxises']
    yaxises = [] if 'yaxises' not in notes else notes['yaxises']

    y_showticklabels = False
    if 'y_tickvals' in notes and 'y_ticktext' in notes:
        if len(notes['y_tickvals']) > 0 and len(notes['y_ticktext']) > 0:
            y_showticklabels = True
    y2_showticklabels = False
    if 'y2_tickvals' in notes and 'y2_ticktext' in notes:
        if len(notes['y2_tickvals']) > 0 and len(notes['y2_ticktext']) > 0:
            y2_showticklabels = True

    traces = []
    bars = data['bar']
    bars_x = bars['x']
    bars_y = bars['y']
    traces.append(Bar(x = bars_x[0], y = bars_y[0], marker = dict(color = bar_colors[0]), name = bar_names[0]))
    for i in range(1, len(bars_y)):
        traces.append(Bar(x = bars_x[i], y = bars_y[i], marker = dict(color = bar_colors[i]), name = bar_names[i]))

    lines = [] if 'scatter' not in data else data['scatter']
    for i in range(0, len(lines)):
        if 'lines' in modes[i]:
            traces.append(Scatter(x = lines[i][0], y = lines[i][1], xaxis = xaxises[i], yaxis = yaxises[i], name = names[i], mode = modes[i], line = dict(color = line_colors[i])))
        elif 'markers' in modes[i]:
            traces.append(Scatter(x = lines[i][0], y = lines[i][1], xaxis = xaxises[i], yaxis = yaxises[i], name = names[i], mode = modes[i], marker = dict(color = line_colors[i])))

    layout = Layout(
        title = '' if 'title' not in notes else notes['title'],
        titlefont = dict(
            size = 36 if 'title_size' not in notes else notes['title_size'],
        ),
        barmode = bar_mode,
        xaxis = dict(
            title = '' if 'x_title' not in notes else notes['x_title'],
            titlefont = dict(
                size = 32 if 'x_title_size' not in notes else notes['x_title_size'],
            ),
            tickfont = dict(
                size = 26 if 'x_tick_size' not in notes else notes['x_tick_size'],
            ),
            linewidth = 2 if 'x_linewidth' not in notes else notes['x_linewidth'],
            dtick = x_dtick,
            mirror = True,
            tickvals = [] if 'x_tickvals' not in notes else notes['x_tickvals'],
            ticktext = [] if 'x_ticktext' not in notes else notes['x_ticktext'],
        ),
        xaxis2 = dict(
            title = '' if 'x2_title' not in notes else notes['x2_title'],
            titlefont = dict(
                size = 32 if 'x2_title_size' not in notes else notes['x2_title_size'],
            ),
            tickfont = dict(
                size = 26 if 'x2_tick_size' not in notes else notes['x2_tick_size'],
            ),
            linewidth = 2 if 'x2_linewidth' not in notes else notes['x2_linewidth'],
            overlaying = 'x' if 'x2_overlaying' not in notes else notes['x2_overlaying'],
            zeroline = True if 'x2_zeroline' not in notes else notes['x2_zeroline'],
            side = 'top' if 'x2_side' not in notes else notes['x2_side'],
            visible = False,
        ),
        yaxis = dict(
            title = '' if 'y_title' not in notes else notes['y_title'],
            titlefont = dict(
                #size = 32 if 'y_title_size' not in notes else notes['y_title_size'],
                size = 28 if 'y_title_size' not in notes else notes['y_title_size'],
            ),
            tickfont = dict(
                size = 26 if 'y_tick_size' not in notes else notes['y_tick_size'],
            ),
            dtick = y_dtick,
            linewidth = 2 if 'y_linewidth' not in notes else notes['y_linewidth'],
            zeroline = True if 'y_zeroline' not in notes else notes['y_zeroline'],
            #overlaying = 'y2',
            mirror = True,
            #showticklabels = y_showticklabels,
            #tickvals = [] if 'y_tickvals' not in notes else notes['y_tickvals'],
            #ticktext = [] if 'y_ticktext' not in notes else notes['y_ticktext'],
        ),
        yaxis2 = dict(
            title = '' if 'y2_title' not in notes else notes['y2_title'],
            titlefont = dict(
                size = 32 if 'y2_title_size' not in notes else notes['y2_title_size'],
            ),
            tickfont = dict(
                size = 26 if 'y2_tick_size' not in notes else notes['y2_tick_size'],
            ),
            dtick = y_dtick if 'y2_dtick' not in notes else notes['y2_dtick'],
            linewidth = 2 if 'y2_linewidth' not in notes else notes['y2_linewidth'],
            overlaying = 'y' if 'y2_overlaying' not in notes else notes['y2_overlaying'],
            zeroline = True if 'y2_zeroline' not in notes else notes['y2_zeroline'],
            side = 'right' if 'y2_side' not in notes else notes['y2_side'],
            visible = False,# if not two_yaxises else True,
            #showticklabels = y2_showticklabels,
            #tickvals = [] if 'y2_tickvals' not in notes else notes['y2_tickvals'],
            #ticktext = [] if 'y2_ticktext' not in notes else notes['y2_ticktext'],
        ),
        margin = dict(
            l = 130 if 'margin_l' not in notes else notes['margin_l'],
            r = 100 if 'margin_r' not in notes else notes['margin_r'],
            b = 80 if 'margin_b' not in notes else notes['margin_b'],
            t = 120 if 'margin_t' not in notes else notes['margin_t'],
            pad = 4 if 'margin_pad' not in notes else notes['margin_pad'],
        ),
        legend = dict(
            x = 0 if 'legend_x' not in notes else notes['legend_x'],
            y = 1 if 'legend_y' not in notes else notes['legend_y'],
            orientation = 'v' if 'legend_orientation' not in notes else notes['legend_orientation'],
            font = dict(
                size = 24 if 'legend_size' not in notes else notes['legend_size'],
            ),
        ),
    )
    fig = Figure(data = traces, layout = layout)
    plot(fig, filename = filename + '.html')
    py.image.save_as(fig, filename = filename + '.png', 
        height = 650 if 'height' not in notes else notes['height'], 
        width = 1120 if 'width' not in notes else notes['width'])


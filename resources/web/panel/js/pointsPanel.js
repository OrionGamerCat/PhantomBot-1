/*
 * Copyright (C) 2017 phantombot.tv
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * @author IllusionaryOne
 */

/*
 * pointsPanel.js
 * Drives the Points Panel
 */
(function() {

    var sortType = 'alpha_asc',
        priceComMods = false,
        modeIcon = [];
        modeIcon['false'] = "<i style=\"color: var(--main-color)\" class=\"fa fa-circle-o\" />";
        modeIcon['true'] = "<i style=\"color: var(--main-color)\" class=\"fa fa-circle\" />";

    /*
     * onMessage
     * This event is generated by the connection (WebSocket) object.
     */
    function onMessage(message) {
        var msgObject,
            groupPointKeys = [ "Caster", "Administrator", "Moderator", "Subscriber", "Donator", "Regular", "Viewer" ];
            timezone = "GMT"; // Default time zone in Core if none given.

        try {
            msgObject = JSON.parse(message.data);
        } catch (ex) {
            return;
        }

        if (panelHasQuery(msgObject)) {
            if (panelCheckQuery(msgObject, 'points_toplist')) {
                $("#topListAmountPoints").val(msgObject['results']['topListAmountPoints']);
            }
            if (panelCheckQuery(msgObject, 'points_settings')) {
                for (idx in msgObject['results']) {
                    var key = "",
                        value = "";

                    key = msgObject['results'][idx]['key'];
                    value = msgObject['results'][idx]['value'];

                    if (panelMatch(key, 'onlineGain')) {
                        $("#setPointGainInput_setgain").val(value);
                    } else if (panelMatch(key, 'offlineGain')) {
                        $("#setPointGainInput_setofflinegain").val(value);
                    } else if (panelMatch(key, 'onlinePayoutInterval')) {
                        $("#setPointGainInput_setinterval").val(value);
                    } else if (panelMatch(key, 'offlinePayoutInterval')) {
                        $("#setPointGainInput_setofflineinterval").val(value);
                    } else if (panelMatch(key, 'pointNameSingle')) {
                        $("#setPointNameInput").val(value);
                    } else if (panelMatch(key, 'pointNameMultiple')) {
                        $("#setPointsNameInput").val(value);
                    } else if (panelMatch(key, 'pointsMessage')) {
                        $("#pointsMessageInput").val(value);
                    } else if (panelMatch(key, 'activeBonus')) {
                        $("#setPointGainInput_setactivebonus").val(value);
                    }
                }
            }

            if (panelCheckQuery(msgObject, 'points_pointstable')) {
                var pointsTableData = msgObject['results'],
                    username = "",
                    points = "",
                    timeValue = "",
                    html = "";

                $("#userPtsTableTitle").html("User Points Table (Refreshing <i class='fa fa-spinner fa-spin' aria-hidden='true'></i>)");

                pointsTableData.sort(sortPointsTable_alpha_asc);

                html = "<table class='table' data-paging='true' data-paging-size='8'" +
                       "       data-filtering='true' data-filter-delay='200'" +
                       "       data-sorting='true'" +
                       "       data-paging-count-format='Rows {PF}-{PL} / {TR}' data-show-header='true'>";
                html += "<thead><tr>" +
                        "    <th data-breakpoints='xs'>Username</th>" +
                        "    <th data-filterable='false' data-type='number'>Points</th>" +
                        "</tr></thead><tbody>";

                for (var idx = 0; idx < pointsTableData.length; idx++) {
                    username = pointsTableData[idx]['key'];
                    points = pointsTableData[idx]['value'];
                    html += "<tr onclick='$.copyUserPoints(\"" + username + "\", \"" + points + "\")' class='textList'>" +
                            "    <td style='width: 50%; cursor: pointer;'>" + username + "</td>" +
                            "    <td style='width: 50%'>" + points + "</td>" +
                            "</tr>";
                }
                html += "</tbody></table>";
                $("#userPointsTable").html(html);
                $('.table').footable({
                    'on': { 'postdraw.ft.table': function(e, ft) { $("#userPtsTableTitle").html("User Points Table"); } }
                });
                handleInputFocus();
            }

            if (panelCheckQuery(msgObject, 'points_pricecommods')) {
                var value = msgObject['results']['pricecomMods'];
                if (value == null || value == undefined) {
                    value = "false";
                }
                priceComMods = value;
                $("#priceComMods").html(modeIcon[value]);
            }

            if (panelCheckQuery(msgObject, 'points_grouppoints')) {
                var groupName = "",
                    groupPoints = "",
                    groupPointsData = [];

                html = "<table>";
                for (var idx = 0; idx < msgObject['results'].length; idx++) {
                    groupName = msgObject['results'][idx]['key'];
                    groupPoints = msgObject['results'][idx]['value'];
                    groupPointsData[groupName] = groupPoints;
                }
                for (key in groupPointKeys) {
                    groupName = groupPointKeys[key];
                    groupPoints = groupPointsData[groupName];

                    html += "<tr class=\"textList\">" +
                            "    <td style=\"width: 15px\">" +
                            "        <div id=\"clearGroupPoints_" + groupName + "\" class=\"button\"" +
                            "             onclick=\"$.updateGroupPoints('" + groupName + "', true, true)\"><i class=\"fa fa-trash\" />" +
                            "        </div>" +
                            "    <td style=\"width: 8em\">" + groupName + "</td>" +
                            "    <td><form onkeypress=\"return event.keyCode != 13\">" +
                            "        <input type=\"number\" min=\"-1\" class=\"input-control\" id=\"inlineGroupPointsEdit_" + groupName + "\"" +
                            "               value=\"" + groupPoints + "\" style=\"width: 5em\"/>" +
                            "        <button type=\"button\" class=\"btn btn-default btn-xs\"" +
                            "               onclick=\"$.updateGroupPoints('" + groupName + "', true, false)\"><i class=\"fa fa-pencil\" />" +
                            "        </button>" +
                            "    </form></td>";

                    if (groupPoints === '-1') {
                        html += "<td style=\"float: right\"><i>Using Global Value</i></td>";
                    } else {
                        html += "<td />";
                    }
                    html += "</tr>";
                }
                $("#groupPointsTable").html(html);
                handleInputFocus();
            }

            if (panelCheckQuery(msgObject, 'points_grouppointsoffline')) {
                var groupName = "",
                    groupPoints = "",
                    groupPointsData = [];

                html = "<table>";
                for (var idx = 0; idx < msgObject['results'].length; idx++) {
                    groupName = msgObject['results'][idx]['key'];
                    groupPoints = msgObject['results'][idx]['value'];
                    groupPointsData[groupName] = groupPoints;
                }
                for (key in groupPointKeys) {
                    groupName = groupPointKeys[key];
                    groupPoints = groupPointsData[groupName];

                    html += "<tr class=\"textList\">" +
                            "    <td style=\"width: 15px\">" +
                            "        <div id=\"clearGroupPointsOffline_" + groupName + "\" class=\"button\"" +
                            "             onclick=\"$.updateGroupPoints('" + groupName + "', false, true)\"><i class=\"fa fa-trash\" />" +
                            "        </div>" +
                            "    <td style=\"width: 8em\">" + groupName + "</td>" +
                            "    <td><form onkeypress=\"return event.keyCode != 13\">" +
                            "        <input type=\"number\" min=\"-1\" class=\"input-control\" id=\"inlineGroupPointsOfflineEdit_" + groupName + "\"" +
                            "               value=\"" + groupPoints + "\" style=\"width: 5em\"/>" +
                            "        <button type=\"button\" class=\"btn btn-default btn-xs\"" +
                            "               onclick=\"$.updateGroupPoints('" + groupName + "', false, false)\"><i class=\"fa fa-pencil\" />" +
                            "        </button>" +
                            "    </form></td>";

                    if (groupPoints === '-1') {
                        html += "<td style=\"float: right\"><i>Using Global Value</i></td>";
                    } else {
                        html += "<td />";
                    }
                    html += "</tr>";
                }
                $("#groupPointsOfflineTable").html(html);
                handleInputFocus();
            }
        }
    }

    /**
     * @function doQuery
     */
    function doQuery() {
        sendDBKeys("points_settings", "pointSettings");
        sendDBKeys("points_pointstable", "points");
        sendDBQuery("points_toplist", "settings", "topListAmountPoints");
        sendDBKeys("points_grouppoints", "grouppoints");
        sendDBQuery("points_pricecommods", "settings", "pricecomMods");
        sendDBKeys("points_grouppointsoffline", "grouppointsoffline");
    }

    /**
     * @function doLiteQuery
     */
    function doLiteQuery() {
        sendDBKeys("points_settings", "pointSettings");
        sendDBQuery("points_toplist", "settings", "topListAmountPoints");
        sendDBKeys("points_grouppoints", "grouppoints");
        sendDBQuery("points_pricecommods", "settings", "pricecomMods");
        sendDBKeys("points_grouppointsoffline", "grouppointsoffline");
    }

    /**
     * @function sortPointsTable
     * @param {Object} a
     * @param {Object} b
     */
    function sortPointsTable_alpha_asc(a, b) {
        return panelStrcmp(a.key, b.key);
    }

    /**
     * @function updateGroupPoints
     * @param {String} group
     * @param {Boolean} online
     * @param {Boolean} clear
     */
    function updateGroupPoints(group, online, clear) {
        var divId = (online ? "#inlineGroupPointsEdit_" + group : "#inlineGroupPointsOfflineEdit_" + group),
            points = (clear ? "-1" : $(divId).val()),
            dbtable = (online ? "grouppoints" : "grouppointsoffline");

        if (points.length > 0) {
            sendDBUpdate("points_updateGroupPoints", dbtable, group, points);
            setTimeout(function() { doLiteQuery(); }, TIMEOUT_WAIT_TIME);
            setTimeout(function() { sendCommand('reloadpoints') }, TIMEOUT_WAIT_TIME);
        }
    }

    /**
     * @function copyUserPoints
     * @param {String} username
     * @param {String} points
     */
    function copyUserPoints(username, points) {
        $("#adjustUserPointsNameInput").val(username);
        $("#adjustUserPointsInput").val(points);
        $('#adjustUserPointsNameInput').focus();
    }

    /**
     * @function setPointName
     */
    function setPointName() {
        var singleName = $("#setPointNameInput").val(),
            pluralName = $("#setPointsNameInput").val();

        if (singleName.match(/\s/ig) || pluralName.match(/\s/ig)) {
            $("#setPointsNameInput").val("Your points name cannot contain a space.");
            setTimeout(function() { doLiteQuery(); }, TIMEOUT_WAIT_TIME * 2);
            return;
        }

        if (singleName.length != 0) {
            sendDBUpdate("points_settings", "pointSettings", "pointNameSingle", singleName);
        }

        if (pluralName.length != 0) {
            sendDBUpdate("points_settings", "pointSettings", "pointNameMultiple", pluralName);
        }

        setTimeout(function() { doLiteQuery(); }, TIMEOUT_WAIT_TIME);
        setTimeout(function() { sendCommand('reloadpoints') }, TIMEOUT_WAIT_TIME);
    }

    /**
     * @function clearPointName
     */
    function clearPointName() {
        sendDBUpdate("points_settings", "pointSettings", "pointNameMultiple", "points");
        sendDBUpdate("points_settings", "pointSettings", "pointNameSingle", "point");
        setTimeout(function() { doLiteQuery(); }, TIMEOUT_WAIT_TIME);
        setTimeout(function() { sendCommand('reloadpoints') }, TIMEOUT_WAIT_TIME);
    }

    /**
     * @function setPointGain
     * @param {String} action
     */
    function setPointGain(action) {
        var value = $("#setPointGainInput_" + action).val();

        if (value.length <= 0) {
            setTimeout(function() { $("#setPointGainInput_" + action).val(''); }, TIMEOUT_WAIT_TIME);
            return;
        }

        if (value.indexOf('.') !== -1 || (parseInt(value) < 1 && parseInt(value) !== 0)) {
            $("#setPointGainInput_" + action).val('Only natural numbers are allowed.');
            setTimeout(function() { $("#setPointGainInput_" + action).val(''); }, TIMEOUT_WAIT_TIME * 3);
            return;
        }

        if (action == "setgain") {
            sendDBUpdate("points_settings", "pointSettings", "onlineGain", value);
        }

        if (action == "setofflinegain") {
            sendDBUpdate("points_settings", "pointSettings", "offlineGain", value);
        }

        if (action == "setinterval") {
            sendDBUpdate("points_settings", "pointSettings", "onlinePayoutInterval", value);
        }

        if (action == "setofflineinterval") {
            sendDBUpdate("points_settings", "pointSettings", "offlinePayoutInterval", value);
        }

        if (action == "setactivebonus") {
            sendDBUpdate("points_settings", "pointSettings", "activeBonus", value);
        }

        setTimeout(function() { doLiteQuery(); }, TIMEOUT_WAIT_TIME);
        setTimeout(function() { sendCommand('reloadpoints') }, TIMEOUT_WAIT_TIME);
    }

    /*
     * @function setPointsMessage
     */
    function setPointsMessage() {
        var value = $("#pointsMessageInput").val();

        if (value.length > 0) {
            sendDBUpdate("points_settings", "pointSettings", "pointsMessage", value);
        }
        setTimeout(function() { doLiteQuery(); }, TIMEOUT_WAIT_TIME);
        setTimeout(function() { sendCommand('reloadpoints') }, TIMEOUT_WAIT_TIME);
    }

    /**
     * @function modifyUserPoints
     * @param {String} action
     */
    function modifyUserPoints(action) {
        var username = $("#adjustUserPointsNameInput").val(),
            points = $("#adjustUserPointsInput").val();
        
        username = username.replace(/\s+/g, '');

        if (action == "take") {
            if (username.length > 0 && points.length > 0) {
                sendDBDecr("points", "points", username.toLowerCase(), String(points));
            }
        }

        if (action == "add") {
            if (username.length > 0 && points.length > 0) {
                sendDBIncr("points", "points", username.toLowerCase(), String(points));
            }
        }

        if (action == "set") {
            if (username.length > 0 && points.length != 0) {
                sendDBUpdate("points", "points", username.toLowerCase(), String(points));
            }
        }
        $("#adjustUserPointsNameInput").val('');
        $("#adjustUserPointsInput").val('');
        setTimeout(function() { doQuery(); }, TIMEOUT_WAIT_TIME);
        setTimeout(function() { sendCommand('reloadpoints') }, TIMEOUT_WAIT_TIME);
    }

    /**
     * @function giftChatPoints
     * @param {String} action
     */
    function giftChatPoints(action) {
        var points = $("#giftChatPointsInput").val(),
            command = "";

        if (points.length > 0) {
            if (panelMatch(action, 'all')) {
                command = "pointsallpanel " + points;
            }
            if (panelMatch(action, 'makeitrain')) {
                command = "makeitrain " + points;
            }
            if (panelMatch(action, 'bonus')) {
                command = "pointsbonuspanel " + points;
            }
            if (panelMatch(action, 'take')) {
                command = "pointstakeallpanel " + points;
            }
            $("#giftChatPointsInput").val('');
            sendCommand(command);
            setTimeout(function() { doQuery(); }, TIMEOUT_WAIT_TIME * 2);
        }
    }

    /**
     * @function giftChatPoints
     * @param {String} action
     */
    function penaltyUser() {
        var user = $("#penaltyUser").val(),
            time = $("#penaltyUserTime").val();

        if (time.length != 0 && user.length != 0) {
            sendCommand('penalty ' + user + ' ' + time);
            $("#penaltyUser").val(user + ' wont gain points for ' + time + ' minutes.');
        } else {
            $("#penaltyUser").val('Error.');
        }
        $("#penaltyUserTime").val('');
        setTimeout(function () { $("#penaltyUser").val(''); }, TIMEOUT_WAIT_TIME * 10);
    }

    /**
     * @function topListPoints
     */
    function topListPoints() {
        var val = $("#topListAmountPoints").val();
        if (val.length != 0) {
            sendDBUpdate("points_toplist", "settings", "topListAmountPoints", val);
        }
        setTimeout(function() { doLiteQuery(); }, TIMEOUT_WAIT_TIME);
        setTimeout(function() { sendCommand('reloadtop'); }, TIMEOUT_WAIT_TIME);
    }

    /**
     * @function toggleModPriceCom
     */
    function toggleModPriceCom() {
        $("#priceComMods").html("<i style=\"color: var(--main-color)\" class=\"fa fa-spinner fa-spin\" />");
        if (priceComMods == "true") {
            sendDBUpdate("points_modprice", "settings", "pricecomMods", "false");
        } else {
            sendDBUpdate("points_modprice", "settings", "pricecomMods", "true");
        }
        setTimeout(function() { doLiteQuery(); }, TIMEOUT_WAIT_TIME);
    }

    // Import the HTML file for this panel.
    $("#pointsPanel").load("/panel/points.html");

    // Load the DB items for this panel, wait to ensure that we are connected.
    var interval = setInterval(function() {
        if (isConnected && TABS_INITIALIZED) {
            var active = $("#tabs").tabs("option", "active");
            if (active == 4) {
                doQuery();
                clearInterval(interval);
            }
        }
    }, INITIAL_WAIT_TIME);

    // Query the DB every 30 seconds for updates.
    setInterval(function() {
        var active = $("#tabs").tabs("option", "active");
        if (active == 4 && isConnected && !isInputFocus()) {
            newPanelAlert('Refreshing Points Data', 'success', 1000);
            doLiteQuery();
        }
    }, 3e4);

    // Export functions - Needed when calling from HTML.
    $.pointsOnMessage = onMessage;
    $.pointsDoQuery = doQuery;
    $.updateGroupPoints = updateGroupPoints;
    $.setPointName = setPointName;
    $.clearPointName = clearPointName;
    $.setPointGain = setPointGain;
    $.giftChatPoints = giftChatPoints;
    $.copyUserPoints = copyUserPoints;
    $.modifyUserPoints = modifyUserPoints;
    $.penaltyUser = penaltyUser;
    $.topListPoints = topListPoints;
    $.toggleModPriceCom = toggleModPriceCom;
    $.setInterval = setInterval;
    $.setPointsMessage = setPointsMessage;
})();

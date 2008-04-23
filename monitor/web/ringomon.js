
jQuery.fn.log = function (msg) {
      console.log("%s: %o", msg, this);
      return this;
 };

$(document).ready(function(){
        $.getJSON("/mon/ring/nodes", update_ring);
        $.ajaxSetup({
                cache: false, 
        })
        var options = {
                callback:filter_domains,
                wait:700,
                highlight:true,
                enterkey:true
        }
        $("#domainfilter").typeWatch(options);

});

function nodename(x){
        var s = x.split("@");
        return [s[0].substr(6), s[1]];
}

function update_ring(ringdata)
{
        var pre_chosen = $("#ringlist > .chosen").attr("node");
        $("#ringlist").html($.map(ringdata, function(node, i){
                var mem = Math.round(node.disk[3] / (1024 * 1024)) + "M";
                var warn = chosen = "";
                if (!node.ok){ warn = "warn "; }
                if (node.node == pre_chosen) { chosen = "chosen "; }
                var N = $.create("div", {"class": "nodenfo"}, [
                        node.disk[2] + " / " + node.disk[0] + " / " +
                        node.disk[1] + " / " + mem]);
                
                nextn = nodename(node.neighbors[1]);
                thisn = nodename(node.node);
                var T = $.create("div", {}, [$.create("span", {"class": "host"},
                        [thisn[1] + ":"]), $.create("span", {"class": "hexid"},
                                [thisn[0] + " \u279D " + nextn[0]])]);
                var R = $.create("div", {"class": "listitem " + warn + chosen},
                        [T, N]);
                $(R).click(function(){
                        $("#ringlist > .chosen").removeClass("chosen");
                        if($(this).hasClass("chosen")){
                                empty_domainlist();
                        }else{
                                $(this).addClass("chosen");
                                $.getJSON("/mon/domains/node?name=" + node.node,
                                        render_domainlist);
                        }
                }).attr("node", node.node);
                return R;
        }));
        $("#ringtitle").html("Ring (" + ringdata.length + " nodes)")

        setTimeout(function(){
                $.getJSON("/mon/ring/nodes", update_ring);
        }, 10000);
}

function empty_domainlist()
{
        $("#domainlist > *").remove();
        $("#domainfilter").val("");
        $("#domaintitle").html("Domains");
}

function render_domainlist(domainlist)
{
        $("#domainlist").html($.map(domainlist, function(d, i){
                R = render_domain(d[3], d[1], d[5], d[0], d[4]);
                $(R).click(function(){
                        $.getJSON("/mon/domains/domain?id=0x" + d[0],
                                render_replicalist);
                });
                return R;
        }));
        var n = domainlist.length;
        $("#domaintitle").html("Domains (showing " + n + "/" + n + ")");
}

function render_replicalist(msg)
{
        var id = msg[0];
        var name = msg[1];
        var chunk = msg[2];
        var domain_info = msg[3];
        var domain = render_domain(chunk, name, domain_info.length, id, true);
        $("#domainbox > .rtitle").html("Domains");
        $(domain).addClass("chosen").click(function(){
                var chosen = $("#ringlist > .chosen");
                if (chosen.length)
                        $.getJSON("/mon/domains/node?name=" +
                                chosen.attr("node"), render_domainlist);
                else{
                        txt = $("#domainfilter").val();
                        $.getJSON("/mon/domains/domain?name=" + escape(txt),
                                render_domainlist);
                }
        });
        domain_info.sort(function(x, y){
                return Number(y.owner) - Number(x.owner)
        });
        var owner = {};
        var replicas = $.map(domain_info, function(d, i){
                var t = "replica on ";
                var st = "";
                if (d.owner){
                        owner = d;
                        t = "owner on ";
                        st = "ownritem";
                } else if (d.error){
                        t = "zombie on ";
                        st = "";
                }
                var thisn = nodename(d.node);
                R = $.create("div", {"class": "listitem replitem"}, [
                               $.create("span", {"class": st}, [t]),
                               $.create("span", {"class": "host"}, [thisn[1] + ":"]),
                               $.create("span", {"class": "hexid"}, [thisn[0]])
                ]);
                $(R).click(function(){
                        $("#domainlist > .replitem").removeClass("chosen");
                        if($(this).hasClass("chosen")){
                                render_domaininfo({});
                        }else{
                                $(this).addClass("chosen");
                                render_domaininfo(domain_info[i], owner);
                        }
                });
                return R;
        });
        $("#domainlist").html([domain].concat(replicas));
}

function render_domain(chunk, name, num_repl, id, alive)
{
        var C = $.create("span", {"class": "chunk"}, ["[" + chunk + "] "]);
        var N = $.create("span", {"class": "domain"}, [name]);
        var H = $.create("div", {"class": "domainnfo"},
                [$.create("span", {}, [num_repl + " / "]),
                $.create("span", {}, [id])]);
        var alivec = ""
        if (!alive)
                alivec = "domain_inactive";
        return $.create("div", {"class": "listitem " + alivec}, [C, N, H]);
}

function filter_domains(txt)
{
        if ($("#ringlist > .chosen").length){
                var all_num = $("#domainlist > *").length;
                $("#domainlist > *").css("display", "none");
                var num = $("#domainlist > *:contains(" + txt + ")")
                        .css("display", "block").length;
                $("#domaintitle").html(
                        "Domains (showing " + num + "/" + all_num + ")");
        }else{
                $.getJSON("/mon/domains/domain?name=" + escape(txt),
                        render_domainlist);
        }
}

function render_domaininfo(stat, owner)
{

        $("#domainstat > table").hide()
        var rows = [];
        var aa = [];
        for (x in stat)
                if (x != "id")
                        aa.push(x)
        $.each(aa, function(i, key){
                
                var ext = "";
                if (typeof(stat[key]) == typeof([]))
                        ext = " \u2b0e";

                var T = $.create("td", {"class": "rowhead"}, [key + ext]);
                var E = [T, $.create("td", {},
                                make_dcell(key, stat[key], true)),
                        $.create("td", {},
                                make_dcell(key, owner[key], true))];

                if (typeof(stat[key]) == typeof([])){
                        var N = [T, $.create("td", {},
                                        make_dcell(key, stat[key], false)),
                                $.create("td", {},
                                        make_dcell(key, owner[key], false))];
                        var R = $.create("tr", {"class": "row_expands"}, N);
                        $(R).click(function(){
                                if ($(this).hasClass("chosen"))
                                        $(this).removeClass("chosen").html(N);
                                else
                                        $(this).addClass("chosen").html(E); 
                        });  
                }else{
                        var R = $.create("tr", {}, E);
                }
                rows.push(R);
        });
        if (rows.length)
                $("#domainstat > table").show()
        $("#domainstat > table > tbody").html(rows);
}

preproc = {"node":
        function (x){
                thisn = nodename(x);
                var X1 = $.create("span", {"class": "host"}, [thisn[1] + ":"]);
                var X2 = $.create("span", {"class": "hexid"}, [thisn[0]]);
                return [X1, X2];
        },
        "started": function (x){ 
                return [$.create("span", 
                        {"class": "tstamp"}, [" (" + x + ")"])];
        },
        "sync_time": function (x) { return [""]; }
};


function make_dcell(key, e, show_all)
{
        if (key in preproc)
                pp = preproc[key];
        else
                pp = function(x) { return [x.toString()]; }

        if (typeof(e) == typeof([])){
                if (show_all)
                        return $.makeArray($.map(e, function(d, i){
                                return $.create("div", {},
                                        val_with_tstamp(pp(d[1]), d[0]));
                        }));
                else
                        return val_with_tstamp(pp(e[0][1]), e[0][0]);
        }else if(e){
                return pp(e);
        }else
                return [""];
}

function val_with_tstamp(v, t){
        return [$.create("span", {}, [v.toString()]),
                $.create("span", {"class": "tstamp"},
                        [" (" + t + ")"])];
}

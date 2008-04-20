
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

function nodename(node){
        s = node.split("@");
        return [s[0].substr(6), s[1]];
}

function update_ring(ringdata)
{
        pre_chosen = $("#ringlist > .chosen").attr("node");
        $("#ringlist").html($.map(ringdata, function(node, i){
                mem = Math.round(node.disk[3] / (1024 * 1024)) + "M";
                warn = chosen = "";
                if (!node.ok){ warn = "warn "; }
                if (node.node == pre_chosen) { chosen = "chosen "; }
                N = $.create("div", {"class": "nodenfo"}, [
                        node.disk[2] + " / " + node.disk[0] + " / " +
                        node.disk[1] + " / " + mem]);
                
                nextn = nodename(node.neighbors[1]);
                thisn = nodename(node.node);
                T = $.create("div", {}, [$.create("span", {"class": "host"}, [thisn[1] + ":"]),
                                         $.create("span", {"class": "hexid"},
                                                [thisn[0] + " \u279D " + nextn[0]])]);
                return $.create("div", {"class": "listitem " + warn + chosen},
                                [T, N]);
        }))
        $("#ringlist > *").each(function(i, N){
                $(N).attr("node", ringdata[i].node)
        }).click(function(){
                if($(this).hasClass("chosen")){
                        empty_domainlist();
                        $(this).removeClass("chosen");
                }else{
                        $("#ringlist > .chosen").removeClass("chosen");
                        $(this).addClass("chosen");
                        $.getJSON("/mon/domains/node?name=" +
                                $(this).attr("node"), render_domainlist);
                }
        });
        
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
                return render_domain(d[3], d[1], d[5], d[0], d[4]);
        }));
        var n = $("#domainlist > *").each(function(i, N){
                $(N).attr("domainid", domainlist[i][0])
        }).click(function(){
                $.getJSON("/mon/domains/domain?id=0x" +
                        $(this).attr("domainid"), render_replicalist);
        }).length;
        $("#domaintitle").html("Domains (showing " + n + "/" + n + ")");
}

function render_replicalist(msg)
{
        var id = msg[0];
        var name = msg[1];
        var chunk = msg[2];
        var repl = msg[3];
        var domain = render_domain(chunk, name, repl.length, id, true);
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
        repl.sort(function(x, y){ return Number(x.owner) - Number(y.owner)});
        var replicas = $.map(repl, function(d, i){
                var t = "replica on ";
                var st = "";
                if (d.owner){
                        t = "owner on ";
                        st = "ownritem";
                }
                var thisn = nodename(d.node);
                return $.create("div", {"class": "listitem replitem"}, [
                               $.create("span", {"class": st}, [t]),
                               $.create("span", {"class": "host"}, [thisn[1] + ":"]),
                               $.create("span", {"class": "hexid"}, [thisn[0]])
                        ]);
        });
        $("#domainlist").html([domain].concat(replicas));

        
        /*
        ["CAACD74A62503A05","adasdasdass32dadasdre32asdas91",0,[{"node":"ringo-5034fb761a4f2270@cfront","id":14604284406132652549,"size":"
undefined","full":false,"owner":false}]]
        */

}

function render_domain(chunk, name, num_repl, id, alive)
{
        C = $.create("span", {"class": "chunk"}, ["[" + chunk + "] "]);
        N = $.create("span", {"class": "domain"}, [name]);
        H = $.create("div", {"class": "domainnfo"},
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

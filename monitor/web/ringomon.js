
jQuery.fn.log = function (msg) {
      console.log("%s: %o", msg, this);
      return this;
 };

$(document).ready(function(){
        $.getJSON("/mon/ring/nodes", update_ring);
        $.ajaxSetup({
                cache: false, 
        }) 
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
                        $(this).addClass("chosen");
                        $.getJSON("/mon/domains/node?name=" +
                                $(this).attr("node"), update_domainlist);
                }
        });
        
        setTimeout(function(){
                $.getJSON("/mon/ring/nodes", update_ring);
        }, 10000);
}

function update_domainlist(domaindata){
        console.log("update domainlist" + domaindata);
        replicas = {}
        showlist = []
        domaindata.sort(function(x, y){ return y[0] - x[0] });
        $.each(domaindata, function(i, D){
                id = D[0]
                if (replicas[id] == undefined){
                        replicas[id] = Array()
                        showlist.push(D);   
                }
                replicas[id].push(D);
        });
        render_domainlist(showlist);
}

function empty_domainlist()
{
        $("#domainlist > *").remove();
}

function render_domainlist(showlist)
{
        $("#domainlist").html($.map(showlist, function(d, i){
                console.log("D:" + d);
                // H = $.create("span", {"class": "hexid"}, [d[0].toString(16)]);
                C = $.create("span", {"class": "chunk"}, ["[" + d[3] + "] "]);
                N = $.create("span", {"class": "domain"}, [d[1]]);
                return $.create("div", {"class": "listitem"}, [C, N]);
        }));
}



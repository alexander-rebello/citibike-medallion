const fs = require("fs");
class N {
  constructor(tag) { this.tag = tag; this.children = []; this.attrs = {}; this.style = {}; this._text = ""; this.parent = null; this.hidden = false; this.listeners = {}; this.id = null; }
  set textContent(v) { this.children = []; this._text = String(v); }
  get textContent() { return this._text + this.children.map(c => c.textContent).join(""); }
  set innerHTML(v) { this.children = []; this._text = "[html]" + v; }
  appendChild(c) { if (c.parent) c.parent.children = c.parent.children.filter(x => x !== c); c.parent = this; this.children.push(c); return c; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  addEventListener(t, f) { this.listeners[t] = f; }
  get clientWidth() { return 900; } get offsetWidth() { return 120; } get offsetHeight() { return 40; }
  getBoundingClientRect() { return { left: 0, top: 0, width: 900, height: 300 }; }
  querySelector(sel) { return new N("tbody"); }
}
const reg = {};
global.document = {
  getElementById: id => reg[id] || (reg[id] = Object.assign(new N("div"), { id })),
  createElement: t => new N(t), createElementNS: (ns, t) => new N(t), createTextNode: t => Object.assign(new N("#text"), { _text: t }),
  querySelector: sel => { const k = "q:" + sel; return reg[k] || (reg[k] = new N("tbody")); },
};
global.window = {};
const html = fs.readFileSync(process.argv[2], "utf8");
const js = html.match(/<script>([\s\S]*?)<\/script>/)[1];
eval(js);
let bad = 0, svgs = 0, paths = 0;
(function walk(n, path) {
  if (n.tag === "svg") svgs++;
  if (n.tag === "path") paths++;
  for (const [k, v] of Object.entries(n.attrs)) if (/NaN|undefined|Infinity/.test(v)) { bad++; console.log("BAD ATTR", n.tag, k, v.slice(0, 80)); }
  for (const [k, v] of Object.entries(n.style)) if (/NaN|undefined/.test(String(v))) { bad++; console.log("BAD STYLE", k, v); }
  n.children.forEach(c => walk(c));
})({ tag: "root", attrs: {}, style: {}, children: Object.values(reg) });
console.log("svgs", svgs, "paths", paths, "bad", bad);
for (const id of ["n-trips", "prov", "foot", "r1", "r2", "r3", "r4", "r5", "r6"]) console.log(id + ":", reg[id].textContent);
// simulate a hover on figure 1 and figure 6 to run the tooltip code
const c1svg = reg["c1"].children[0]; const rect = c1svg.children[c1svg.children.length - 1];
rect.listeners.pointermove({ clientX: 700, clientY: 100 }); console.log("tooltip after hover:", reg["tip"] ? reg["tip"].textContent.slice(0, 90) : "none");

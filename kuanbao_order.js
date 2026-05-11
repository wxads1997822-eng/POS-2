/**
 * Kuanbao 客用外帶點餐頁邏輯。
 * 寫入 orders 的欄位必須與 firestore.rules 內 isCustomerPendingMobileOrder() 白名單一致。
 */
import { initializeApp } from "https://www.gstatic.com/firebasejs/12.12.1/firebase-app.js";
import {
  getFirestore,
  collection,
  getDocs,
  addDoc,
  Timestamp,
  query,
  orderBy,
  doc,
  onSnapshot
} from "https://www.gstatic.com/firebasejs/12.12.1/firebase-firestore.js";

const firebaseConfig = {
  apiKey: "AIzaSyD-9cqAyRjtUxbJhV95o5Y203oOhc3Twew",
  authDomain: "guanbao-pos.firebaseapp.com",
  projectId: "guanbao-pos",
  storageBucket: "guanbao-pos.firebasestorage.app",
  messagingSenderId: "1032609415697",
  appId: "1:1032609415697:web:d39485dd3df274606a67b0",
  measurementId: "G-0F3DDD34K1"
};

const ORDERS_COLLECTION = "orders";
const PRODUCTS_COLLECTION = "products";
const CONFIG_COLLECTION = "config";
const STORE_STATUS_DOC_ID = "store_status";

/** 與 Firestore 規則檢查一致（shop_name 長度 1–40） */
const SHOP_NAME = "Kuanbao";

/** 與 isCustomerPendingMobileOrder 完全一致 */
const TABLE_NUMBER_ONLINE = "線上外帶";
const PAYMENT_METHOD = "pay_at_store";
const ORDER_SOURCE = "customer_mobile";
const ORDER_STATUS = "pending";

const COOLDOWN_MS = 5 * 60 * 1000;
const COOLDOWN_STORAGE_KEY = "kuanbao_order_last_submit_ts_v1";

/** 送單後追蹤用：重新整理仍可回到追蹤畫面 */
const TRACK_ORDER_ID_KEY = "kuanbao_order_track_id_v1";

const SUCCESS_MESSAGE = "訂單已送出，請等候 Kuanbao 店家確認！";

/** 店內專用折扣／負價品 ID，客用頁一律不顯示、不可送出 */
const CUSTOMER_EXCLUDED_PRODUCT_IDS = new Set(["eco_discount"]);

const app = initializeApp(firebaseConfig);
const db = getFirestore(app);

let storeAcceptingOnlineOrders = true;
let storeStatusUnsubscribe = null;
let orderTrackUnsubscribe = null;

/** 線上點餐可取餐窗：06:30 起，最後取餐時段 13:40（與選單一致） */
const ONLINE_PICKUP_START_H = 6;
const ONLINE_PICKUP_START_M = 30;
const ONLINE_PICKUP_LAST_H = 13;
const ONLINE_PICKUP_LAST_M = 40;
const MIN_LEAD_MINUTES = 15;

const MSG_DAILY_ONLINE_ENDED = "今日線上收單已結束，歡迎明日 06:30 後再點餐";
const PAUSE_MSG_STORE_DEFAULT = "目前忙碌中，暫停線上點餐";

/** 當日已無任何合法取餐時段（含超過 13:40 最後時段） */
let dailyOnlineOrderingClosed = false;

function pad2(n) {
  return String(n).padStart(2, "0");
}

function formatTimeHHMM(d) {
  return pad2(d.getHours()) + ":" + pad2(d.getMinutes());
}

function atTodayHM(baseDate, h, m) {
  const d = new Date(baseDate);
  d.setHours(h, m, 0, 0);
  d.setMilliseconds(0);
  return d;
}

/**
 * 第一個可選時段：嚴格不早於「現在 + leadMin 分鐘」，且分鐘向上對齊 15 分鐘刻度。
 */
function firstPickupSlotAfterMinLead(now, leadMin) {
  const lead = Number(leadMin);
  const lm = Number.isFinite(lead) && lead > 0 ? lead : MIN_LEAD_MINUTES;
  const deadlineMs = now.getTime() + lm * 60000;
  let t = new Date(deadlineMs);
  t.setMilliseconds(0);
  t.setSeconds(0, 0);
  if (t.getTime() < deadlineMs) {
    t.setMinutes(t.getMinutes() + 1);
    t.setSeconds(0, 0);
  }
  let m = t.getMinutes();
  const mod = m % 15;
  if (mod !== 0) {
    t.setMinutes(m + (15 - mod));
    t.setSeconds(0, 0);
  }
  while (t.getTime() < deadlineMs) {
    t = new Date(t.getTime() + 15 * 60000);
  }
  return t;
}

/**
 * 回傳當日 06:30–13:40 內可取餐時段（15 分鐘一格）。
 * 早鳥：06:30 以前開頁，最早選項固定自 06:45 起。
 * 其餘：第一個實際時段不早於「現在 + 15 分鐘」並對齊 15 分鐘（例 11:00 點餐最快 11:15）。
 * closedForDay：起算後第一個合法時段已晚於 13:40（今日無法再接單）。
 */
function buildPickupTimeSlotDates(now) {
  const ref = now && now instanceof Date ? now : new Date();
  const dayOpen = atTodayHM(ref, ONLINE_PICKUP_START_H, ONLINE_PICKUP_START_M);
  const earlyBirdFirstSlot = atTodayHM(ref, 6, 45);
  const dayLast = atTodayHM(ref, ONLINE_PICKUP_LAST_H, ONLINE_PICKUP_LAST_M);

  let first;
  if (ref.getTime() < dayOpen.getTime()) {
    first = new Date(earlyBirdFirstSlot);
  } else {
    first = firstPickupSlotAfterMinLead(ref, MIN_LEAD_MINUTES);
    if (first.getTime() < dayOpen.getTime()) {
      first = new Date(dayOpen);
    }
  }

  if (first.getTime() > dayLast.getTime()) {
    return { slots: [], closedForDay: true };
  }

  const slots = [];
  let t = new Date(first);
  let guard = 0;
  while (t.getTime() <= dayLast.getTime() && guard < 200) {
    guard += 1;
    slots.push(new Date(t));
    t = new Date(t.getTime() + 15 * 60000);
  }
  return { slots, closedForDay: slots.length === 0 };
}

function getCurrentPickupSlotValueSet(now) {
  const { slots } = buildPickupTimeSlotDates(now || new Date());
  return new Set(slots.map((d) => formatTimeHHMM(d)));
}

function fillPickupTimeSelect(selectEl) {
  if (!selectEl) return;
  const { slots, closedForDay } = buildPickupTimeSlotDates(new Date());
  dailyOnlineOrderingClosed = closedForDay || slots.length === 0;
  selectEl.innerHTML = "";

  if (dailyOnlineOrderingClosed) {
    const ph = document.createElement("option");
    ph.value = "";
    ph.textContent = "今日已無可取餐時段";
    selectEl.appendChild(ph);
    selectEl.disabled = true;
    return;
  }

  selectEl.disabled = false;
  const ph = document.createElement("option");
  ph.value = "";
  ph.textContent = "請選擇取餐時間";
  selectEl.appendChild(ph);
  slots.forEach((d) => {
    const opt = document.createElement("option");
    const v = formatTimeHHMM(d);
    opt.value = v;
    opt.textContent = v;
    selectEl.appendChild(opt);
  });
}

function applyStorePausedUi() {
  const pauseEl = document.getElementById("pauseOrderOverlay");
  const dock = document.getElementById("bottomDock");
  if (!pauseEl || !dock) return;
  const pauseTextEl = pauseEl.querySelector(".pause-order-text");
  if (document.body.classList.contains("mode-order-tracking")) {
    pauseEl.hidden = true;
    dock.hidden = true;
    return;
  }
  if (dailyOnlineOrderingClosed) {
    if (pauseTextEl) pauseTextEl.textContent = MSG_DAILY_ONLINE_ENDED;
    pauseEl.hidden = false;
    dock.hidden = true;
    document.body.classList.add("daily-order-ended");
    document.body.classList.remove("store-paused");
    return;
  }
  document.body.classList.remove("daily-order-ended");
  if (storeAcceptingOnlineOrders) {
    pauseEl.hidden = true;
    dock.hidden = false;
    document.body.classList.remove("store-paused");
    if (pauseTextEl) pauseTextEl.textContent = PAUSE_MSG_STORE_DEFAULT;
  } else {
    if (pauseTextEl) pauseTextEl.textContent = PAUSE_MSG_STORE_DEFAULT;
    pauseEl.hidden = false;
    dock.hidden = true;
    document.body.classList.add("store-paused");
  }
}

function saveTrackOrderId(id) {
  try {
    localStorage.setItem(TRACK_ORDER_ID_KEY, String(id));
  } catch (e) {
    /* ignore */
  }
}

function getTrackOrderId() {
  try {
    const raw = localStorage.getItem(TRACK_ORDER_ID_KEY);
    if (!raw || !String(raw).trim()) return null;
    return String(raw).trim();
  } catch (e) {
    return null;
  }
}

function clearTrackOrderId() {
  try {
    localStorage.removeItem(TRACK_ORDER_ID_KEY);
  } catch (e) {
    /* ignore */
  }
}

function stopOrderTrackListener() {
  if (orderTrackUnsubscribe) {
    orderTrackUnsubscribe();
    orderTrackUnsubscribe = null;
  }
}

function formatOrderNoDisplay(n) {
  const num = Number(n);
  if (!Number.isFinite(num) || num <= 0) return null;
  return String(Math.round(num)).padStart(3, "0");
}

function buildTrackDetailLines(data) {
  const lines = [];
  const pt = data && typeof data.pickup_time === "string" ? data.pickup_time.trim() : "";
  if (pt) lines.push("預計取餐時間：" + pt);
  const no = formatOrderNoDisplay(data && data.orderNo);
  if (no) lines.push("取餐號碼：#" + no);
  return lines.join("\n");
}

function renderTrackContent(statusBox, detailEl, snap) {
  if (!snap.exists()) {
    statusBox.className = "order-track-status order-track-status--cancel";
    statusBox.textContent = "❌ 訂單已被取消，若有疑問請聯繫櫃台。";
    detailEl.textContent = "";
    return;
  }
  const d = snap.data();
  const status = typeof d.status === "string" ? d.status : "";
  const detail = buildTrackDetailLines(d);

  if (status === "voided") {
    statusBox.className = "order-track-status order-track-status--cancel";
    statusBox.textContent = "❌ 訂單已被取消，若有疑問請聯繫櫃台。";
    detailEl.textContent = "";
    return;
  }

  if (status === "active") {
    statusBox.className = "order-track-status order-track-status--active";
    statusBox.textContent = "✅ 店家已接單！";
    detailEl.textContent = detail || "正在準備中，請依約前往取餐。";
    return;
  }

  if (status === "pending") {
    statusBox.className = "order-track-status order-track-status--pending";
    statusBox.textContent = "⏳ 訂單已送出，等待店家確認中...";
    detailEl.textContent = detail || "請稍候，店家確認後將更新為已接單。";
    return;
  }

  statusBox.className = "order-track-status order-track-status--cancel";
  statusBox.textContent = "❌ 訂單已被取消，若有疑問請聯繫櫃台。";
  detailEl.textContent = "";
}

function startStoreStatusListener(pickupTimeSelect, syncSubmitState) {
  if (storeStatusUnsubscribe) return;
  const ref = doc(db, CONFIG_COLLECTION, STORE_STATUS_DOC_ID);
  let previousOpen = storeAcceptingOnlineOrders;
  storeStatusUnsubscribe = onSnapshot(
    ref,
    (snap) => {
      const open = !snap.exists() || snap.data().is_accepting_orders !== false;
      if (open && !previousOpen && pickupTimeSelect) {
        fillPickupTimeSelect(pickupTimeSelect);
      }
      previousOpen = open;
      storeAcceptingOnlineOrders = open;
      applyStorePausedUi();
      if (typeof syncSubmitState === "function") syncSubmitState();
    },
    (err) => {
      console.error("接單狀態監聽失敗：", err);
    }
  );
}

/** 僅含規則允許的 16 個鍵，避免多餘欄位遭拒 */
function buildPendingCustomerOrderDocument({
  date,
  month,
  total,
  itemCounts,
  checkoutTime,
  lineItems,
  customer_name,
  customer_phone,
  pickup_time
}) {
  const pt = typeof pickup_time === "string" ? pickup_time.trim() : "";
  return {
    date,
    month,
    total,
    itemCounts,
    timestamp: Timestamp.fromDate(checkoutTime),
    orderNo: 0,
    table_number: TABLE_NUMBER_ONLINE,
    pickup_time: pt,
    status: ORDER_STATUS,
    lineItems,
    is_daily_cleared: false,
    source: ORDER_SOURCE,
    customer_name,
    customer_phone,
    payment_method: PAYMENT_METHOD,
    shop_name: SHOP_NAME
  };
}

function formatMoney(n) {
  return "$" + Math.round(n);
}

function isCooldownActive() {
  try {
    const raw = localStorage.getItem(COOLDOWN_STORAGE_KEY);
    if (!raw) return false;
    const ts = Number(raw);
    if (!Number.isFinite(ts)) return false;
    return Date.now() - ts < COOLDOWN_MS;
  } catch (e) {
    return false;
  }
}

function cooldownRemainingSec() {
  try {
    const raw = localStorage.getItem(COOLDOWN_STORAGE_KEY);
    const ts = Number(raw);
    if (!Number.isFinite(ts)) return 0;
    return Math.max(0, Math.ceil((COOLDOWN_MS - (Date.now() - ts)) / 1000));
  } catch (e) {
    return 0;
  }
}

function markSubmittedNow() {
  try {
    localStorage.setItem(COOLDOWN_STORAGE_KEY, String(Date.now()));
  } catch (e) {
    /* ignore */
  }
}

/** 客用點餐可選品（店內 POS 不受此限制）：折扣品與非正價一律不可見、不可結帳 */
function isProductVisibleToCustomer(p) {
  if (!p || typeof p.id !== "string" || !p.id) return false;
  if (p.discount === true) return false;
  if (CUSTOMER_EXCLUDED_PRODUCT_IDS.has(p.id)) return false;
  const pr = Number(p.price);
  if (!Number.isFinite(pr) || pr <= 0) return false;
  return true;
}

function normalizeProductDoc(docSnap) {
  const data = docSnap.data();
  if (!data || typeof data !== "object") return null;
  if (data.enabled === false) return null;
  const name = typeof data.name === "string" ? data.name.trim() : "";
  const price = Number(data.price);
  if (!name || !Number.isFinite(price)) return null;
  const id = typeof data.id === "string" && data.id.trim() ? data.id.trim() : docSnap.id;
  if (data.discount === true || price <= 0 || id === "eco_discount") return null;
  const sortOrder = Number(data.sortOrder);
  const product = {
    id,
    name,
    price,
    discount: false,
    sortOrder: Number.isFinite(sortOrder) ? sortOrder : 1e9
  };
  if (!isProductVisibleToCustomer(product)) return null;
  return product;
}

function escapeAttr(s) {
  return String(s).replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/</g, "&lt;");
}

function validatePhoneTw(tel) {
  return /^09\d{8}$/.test(String(tel).trim());
}

function getTodayKey(d) {
  const t = d || new Date();
  return (
    t.getFullYear() +
    "-" +
    String(t.getMonth() + 1).padStart(2, "0") +
    "-" +
    String(t.getDate()).padStart(2, "0")
  );
}

function getMonthKey(d) {
  const t = d || new Date();
  return t.getFullYear() + "-" + String(t.getMonth() + 1).padStart(2, "0");
}

/** @type {{ id: string, name: string, price: number, sortOrder: number }[]} */
let products = [];
/** @type {Record<string, number>} */
let quantities = {};

function cartTotal() {
  let sum = 0;
  products.forEach((p) => {
    if (!isProductVisibleToCustomer(p)) return;
    const q = quantities[p.id] || 0;
    sum += p.price * q;
  });
  return sum;
}

function buildLineItemsAndCounts() {
  const itemCounts = {};
  const lineItems = [];
  products.forEach((p) => {
    if (!isProductVisibleToCustomer(p)) return;
    const q = quantities[p.id] || 0;
    if (q <= 0) return;
    itemCounts[p.id] = q;
    for (let i = 0; i < q; i += 1) {
      lineItems.push({ id: p.id, name: p.name, price: p.price });
    }
  });
  return { itemCounts, lineItems };
}

/** 送出前複驗：不得含折扣品、負價／零價或排除 ID（防竄改 quantities） */
function assertCartAllowedForCustomerSubmit(itemCounts, lineItems) {
  const allowedIds = new Set(products.filter(isProductVisibleToCustomer).map((p) => p.id));
  for (const id of Object.keys(itemCounts)) {
    const n = itemCounts[id];
    if (!Number.isFinite(n) || n <= 0) return false;
    const prod = products.find((x) => x && x.id === id);
    if (!prod || !isProductVisibleToCustomer(prod)) return false;
    const linesWithId = lineItems.filter((l) => l && l.id === id).length;
    if (linesWithId !== n) return false;
  }
  for (const line of lineItems) {
    if (!line || typeof line.id !== "string") return false;
    if (!allowedIds.has(line.id)) return false;
    const pr = Number(line.price);
    if (!Number.isFinite(pr) || pr <= 0) return false;
    const prod = products.find((x) => x && x.id === line.id);
    if (!prod || !isProductVisibleToCustomer(prod)) return false;
  }
  return true;
}

function pruneQuantitiesToVisibleProducts() {
  const keep = new Set(products.filter(isProductVisibleToCustomer).map((p) => p.id));
  Object.keys(quantities).forEach((id) => {
    if (!keep.has(id)) delete quantities[id];
  });
}

function initKuanbaoOrderPage() {
  const menuList = document.getElementById("menuList");
  const cartTotalEl = document.getElementById("cartTotal");
  const custName = document.getElementById("custName");
  const custPhone = document.getElementById("custPhone");
  const pickupTimeSelect = document.getElementById("pickupTimeSelect");
  const bottomDock = document.getElementById("bottomDock");
  const pauseOrderOverlay = document.getElementById("pauseOrderOverlay");
  const submitBtn = document.getElementById("submitBtn");
  const formMsg = document.getElementById("formMsg");
  const successOverlay = document.getElementById("successOverlay");
  const successDismissBtn = document.getElementById("successDismissBtn");
  const orderTrackStatusBox = document.getElementById("orderTrackStatusBox");
  const orderTrackDetail = document.getElementById("orderTrackDetail");
  const orderTrackDismissBtn = document.getElementById("orderTrackDismissBtn");

  if (
    !menuList ||
    !cartTotalEl ||
    !custName ||
    !custPhone ||
    !pickupTimeSelect ||
    !bottomDock ||
    !pauseOrderOverlay ||
    !submitBtn ||
    !formMsg ||
    !orderTrackStatusBox ||
    !orderTrackDetail ||
    !orderTrackDismissBtn
  ) {
    console.error("Kuanbao order page: missing DOM nodes.");
    return;
  }

  function startOrderTrackListener(orderId) {
    stopOrderTrackListener();
    const ref = doc(db, ORDERS_COLLECTION, orderId);
    orderTrackUnsubscribe = onSnapshot(
      ref,
      (snap) => {
        renderTrackContent(orderTrackStatusBox, orderTrackDetail, snap);
      },
      (err) => {
        console.error("訂單追蹤監聽失敗：", err);
        stopOrderTrackListener();
        clearTrackOrderId();
        document.body.classList.remove("mode-order-tracking");
        setMsg("無法同步訂單狀態，已返回點餐畫面。若訂單仍存在請聯繫櫃台。", true);
        applyStorePausedUi();
        syncSubmitState();
      }
    );
  }

  function updateTotal() {
    cartTotalEl.textContent = formatMoney(cartTotal());
  }

  function renderMenu() {
    menuList.className = "";
    menuList.innerHTML = "";
    products.forEach((p) => {
      if (p.discount === true || Number(p.price) <= 0) return;
      const qty = quantities[p.id] || 0;
      const row = document.createElement("div");
      row.className = "menu-row";
      row.innerHTML =
        '<div class="menu-main">' +
        '<div class="menu-name"></div>' +
        '<div class="menu-price"></div>' +
        "</div>" +
        '<div class="qty-controls">' +
        '<button type="button" class="qty-btn" data-act="minus" data-id="' +
        escapeAttr(p.id) +
        '" aria-label="減少">−</button>' +
        '<span class="qty-val">' +
        qty +
        "</span>" +
        '<button type="button" class="qty-btn" data-act="plus" data-id="' +
        escapeAttr(p.id) +
        '" aria-label="增加">+</button>' +
        "</div>";
      row.querySelector(".menu-name").textContent = p.name;
      row.querySelector(".menu-price").textContent = formatMoney(p.price);
      menuList.appendChild(row);
    });

    menuList.querySelectorAll(".qty-btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        const id = btn.getAttribute("data-id");
        const act = btn.getAttribute("data-act");
        if (!id) return;
        const cur = quantities[id] || 0;
        if (act === "plus") quantities[id] = cur + 1;
        if (act === "minus") quantities[id] = Math.max(0, cur - 1);
        renderMenu();
        updateTotal();
        syncSubmitState();
      });
    });
  }

  function setMsg(text, isError) {
    formMsg.textContent = text || "";
    formMsg.className = isError ? "dock-msg dock-msg--error" : "dock-msg";
  }

  function syncSubmitState() {
    if (document.body.classList.contains("mode-order-tracking")) {
      submitBtn.disabled = true;
      formMsg.textContent = "";
      return;
    }
    const { lineItems } = buildLineItemsAndCounts();
    const hasItems = lineItems.length > 0;
    const nameOk = custName.value.trim().length > 0;
    const phoneOk = validatePhoneTw(custPhone.value);
    const menuOk = products.some(isProductVisibleToCustomer);
    const pickupOk =
      !pickupTimeSelect.disabled && pickupTimeSelect.value.trim() !== "";
    const cd = isCooldownActive();
    const canOrder = storeAcceptingOnlineOrders && !dailyOnlineOrderingClosed;
    submitBtn.disabled = !canOrder || !hasItems || !nameOk || !phoneOk || !menuOk || !pickupOk || cd;
    if (dailyOnlineOrderingClosed) {
      setMsg(MSG_DAILY_ONLINE_ENDED, true);
    } else if (!canOrder) {
      formMsg.textContent = "";
    } else if (cd) {
      setMsg("同一裝置須間隔 5 分鐘才能再送單，約剩 " + cooldownRemainingSec() + " 秒。", true);
    } else if (!menuOk) {
      formMsg.textContent = "";
    } else {
      formMsg.textContent = "";
    }
  }

  async function loadProducts() {
    let snap;
    try {
      snap = await getDocs(query(collection(db, PRODUCTS_COLLECTION), orderBy("sortOrder", "asc")));
    } catch (errOrder) {
      try {
        snap = await getDocs(collection(db, PRODUCTS_COLLECTION));
      } catch (err) {
        console.error(err);
        menuList.className = "empty-menu";
        menuList.textContent =
          "無法載入菜單。請確認 Firestore 已有 products 集合，且規則允許讀取；若使用 sortOrder 排序請建立索引。";
        syncSubmitState();
        return;
      }
    }

    const list = [];
    snap.forEach((docSnap) => {
      const p = normalizeProductDoc(docSnap);
      if (p) list.push(p);
    });
    products = list
      .sort((a, b) => {
        if (a.sortOrder !== b.sortOrder) return a.sortOrder - b.sortOrder;
        return a.name.localeCompare(b.name, "zh-Hant");
      })
      .filter(isProductVisibleToCustomer);
    pruneQuantitiesToVisibleProducts();

    if (products.length === 0) {
      menuList.className = "empty-menu";
      menuList.textContent = "目前沒有可點餐的品項，請稍後再試或聯絡 Kuanbao。";
    } else {
      renderMenu();
    }
    updateTotal();
    syncSubmitState();
  }

  async function submitOrder() {
    formMsg.textContent = "";
    const name = custName.value.trim();
    const phone = custPhone.value.replace(/\D/g, "").slice(0, 10);
    custPhone.value = phone;

    if (!name) {
      setMsg("請填寫姓名。", true);
      return;
    }
    if (!validatePhoneTw(phone)) {
      setMsg("手機號碼須為 09 開頭的 10 位數字。", true);
      return;
    }
    if (!storeAcceptingOnlineOrders) {
      setMsg("目前暫停線上點餐。", true);
      return;
    }
    if (dailyOnlineOrderingClosed) {
      window.alert(MSG_DAILY_ONLINE_ENDED);
      setMsg(MSG_DAILY_ONLINE_ENDED, true);
      return;
    }
    const pickupVal = pickupTimeSelect.value.trim();
    if (!pickupVal) {
      window.alert("請選擇預計取餐時間");
      setMsg("請選擇預計取餐時間", true);
      return;
    }
    const allowedPickups = getCurrentPickupSlotValueSet(new Date());
    if (!allowedPickups.has(pickupVal)) {
      setMsg("取餐時間已失效，請重新選擇時段。", true);
      fillPickupTimeSelect(pickupTimeSelect);
      applyStorePausedUi();
      syncSubmitState();
      return;
    }
    if (isCooldownActive()) {
      setMsg("送出過於頻繁，請稍後再試。", true);
      return;
    }

    const { itemCounts, lineItems } = buildLineItemsAndCounts();
    if (lineItems.length === 0) {
      setMsg("請至少選擇一項品項。", true);
      return;
    }
    if (!assertCartAllowedForCustomerSubmit(itemCounts, lineItems)) {
      setMsg("訂單含無效品項或店內專用折扣，請重新選擇後再送出。", true);
      pruneQuantitiesToVisibleProducts();
      renderMenu();
      updateTotal();
      syncSubmitState();
      return;
    }

    const now = new Date();
    const total = lineItems.reduce((s, it) => s + it.price, 0);

    const payload = buildPendingCustomerOrderDocument({
      date: getTodayKey(now),
      month: getMonthKey(now),
      total,
      itemCounts,
      checkoutTime: now,
      lineItems,
      customer_name: name,
      customer_phone: phone,
      pickup_time: pickupVal
    });

    submitBtn.disabled = true;
    try {
      const docRef = await addDoc(collection(db, ORDERS_COLLECTION), payload);
      markSubmittedNow();
      quantities = {};
      renderMenu();
      updateTotal();
      custName.value = "";
      custPhone.value = "";
      fillPickupTimeSelect(pickupTimeSelect);
      saveTrackOrderId(docRef.id);
      document.body.classList.add("mode-order-tracking");
      applyStorePausedUi();
      startOrderTrackListener(docRef.id);
    } catch (err) {
      console.error(err);
      setMsg("送出失敗：" + (err && err.message ? err.message : String(err)), true);
    } finally {
      syncSubmitState();
    }
  }

  custName.addEventListener("input", syncSubmitState);
  custPhone.addEventListener("input", () => {
    custPhone.value = custPhone.value.replace(/\D/g, "").slice(0, 10);
    syncSubmitState();
  });
  pickupTimeSelect.addEventListener("change", syncSubmitState);
  submitBtn.addEventListener("click", () => {
    void submitOrder();
  });

  if (successOverlay && successDismissBtn) {
    successDismissBtn.addEventListener("click", () => {
      successOverlay.hidden = true;
      document.body.classList.remove("success-open");
      syncSubmitState();
    });
    const successTextEl = successOverlay.querySelector(".success-text");
    if (successTextEl) successTextEl.textContent = SUCCESS_MESSAGE;
  }

  orderTrackDismissBtn.addEventListener("click", () => {
    clearTrackOrderId();
    stopOrderTrackListener();
    document.body.classList.remove("mode-order-tracking");
    fillPickupTimeSelect(pickupTimeSelect);
    applyStorePausedUi();
    syncSubmitState();
    void loadProducts();
  });

  setInterval(() => {
    if (isCooldownActive()) syncSubmitState();
  }, 1000);

  function refreshPickupWindowAndUi() {
    if (document.body.classList.contains("mode-order-tracking")) return;
    fillPickupTimeSelect(pickupTimeSelect);
    applyStorePausedUi();
    syncSubmitState();
  }
  window.setInterval(refreshPickupWindowAndUi, 30000);
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") refreshPickupWindowAndUi();
  });

  fillPickupTimeSelect(pickupTimeSelect);

  const existingTrackId = getTrackOrderId();
  if (existingTrackId) {
    document.body.classList.add("mode-order-tracking");
  }

  applyStorePausedUi();
  startStoreStatusListener(pickupTimeSelect, syncSubmitState);

  updateTotal();
  if (existingTrackId) {
    startOrderTrackListener(existingTrackId);
  }
  void loadProducts();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", () => initKuanbaoOrderPage(), { once: true });
} else {
  initKuanbaoOrderPage();
}

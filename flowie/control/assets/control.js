(function () {
  "use strict";

  var FOCUSABLE_SELECTOR = [
    "button:not([disabled])",
    "input:not([disabled]):not([type=hidden])",
    "select:not([disabled])",
    "textarea:not([disabled])",
    "a[href]",
    "summary",
    "[tabindex]:not([tabindex='-1'])"
  ].join(",");
  var activePopover = null;

  function resetRequestIds(scope) {
    if (!scope) return;
    scope.querySelectorAll("[data-request-id]").forEach(function (requestId) {
      requestId.value = "";
    });
  }

  function getFocusable(container) {
    return Array.from(container.querySelectorAll(FOCUSABLE_SELECTOR)).filter(function (element) {
      return !element.hidden && element.getClientRects().length > 0;
    });
  }

  function deferFocus(callback) {
    if (typeof window.requestAnimationFrame === "function") {
      window.requestAnimationFrame(callback);
    } else {
      window.setTimeout(callback, 0);
    }
  }

  function pickerOptions(list) {
    return Array.from(list.querySelectorAll("[data-picker-option]:not(:disabled)"));
  }

  function setPickerTabStops(list, active) {
    list.querySelectorAll("[data-picker-option]").forEach(function (candidate) {
      candidate.setAttribute("tabindex", candidate === active ? "0" : "-1");
    });
  }

  function initializePickerList(list) {
    var options = pickerOptions(list);
    var active = options.find(function (candidate) {
      return candidate.getAttribute("aria-selected") === "true";
    }) || options[0];

    if (active) setPickerTabStops(list, active);
  }

  function adjacentPickerOption(option, key) {
    var list = option.closest("[role=listbox]");
    var options;
    var index;

    if (!list) return null;
    options = pickerOptions(list);
    index = options.indexOf(option);
    if (index < 0) return null;
    if (key === "Home") return options[0];
    if (key === "End") return options[options.length - 1];
    if (key === "ArrowDown") return options[Math.min(index + 1, options.length - 1)];
    if (key === "ArrowUp") return options[Math.max(index - 1, 0)];
    return null;
  }

  function selectOption(option) {
    var picker = option.closest("[data-entity-picker]");
    var list = option.closest("[role=listbox]");
    var labelNode;
    var value;
    var label;

    if (!picker || !list || option.disabled) return;
    value = option.getAttribute("data-value") || "";
    labelNode = option.querySelector("span");
    label = (labelNode ? labelNode.textContent : option.textContent).trim();
    list.querySelectorAll("[data-picker-option]").forEach(function (candidate) {
      candidate.setAttribute("aria-selected", candidate === option ? "true" : "false");
    });
    setPickerTabStops(list, option);
    picker.querySelectorAll("[data-picker-target]").forEach(function (target) {
      target.value = value;
    });
    picker.querySelectorAll("[data-picker-output]").forEach(function (output) {
      output.textContent = label;
      output.classList.remove("entity-picker__selection--empty");
    });
    picker.querySelectorAll("[data-picker-action]").forEach(function (action) {
      action.disabled = false;
    });
    resetRequestIds(picker);
  }

  function loadPickerOptions(target) {
    if (!target) return;
    target.querySelectorAll("[data-picker-options]").forEach(function (list) {
      var templateId = list.getAttribute("data-picker-options");
      var template = templateId ? document.getElementById(templateId) : null;
      if (!template || list.hasAttribute("data-loaded")) return;
      list.replaceChildren(template.content.cloneNode(true));
      list.setAttribute("data-loaded", "");
      initializePickerList(list);
    });
  }

  function focusPopover(popover) {
    var target = popover.querySelector("[data-picker-option][tabindex='0']") ||
                 popover.querySelector("input:not([type=hidden]):not([disabled]), select:not([disabled]), textarea:not([disabled])") ||
                 popover.querySelector("button:not(.button--close):not([disabled])") ||
                 popover.querySelector(".button--close") ||
                 getFocusable(popover)[0];

    if (target) target.focus();
  }

  // Keep native popovers keyboard-contained while preserving the invoking control's focus.
  function setPopoverBackgroundInert(popover) {
    var current = popover;
    var inertNodes = [];

    while (current && current.parentElement && current.parentElement !== document.documentElement) {
      Array.from(current.parentElement.children).forEach(function (sibling) {
        if (sibling === current || sibling.inert) return;
        sibling.inert = true;
        inertNodes.push(sibling);
      });
      current = current.parentElement;
    }
    popover.__controlInertNodes = inertNodes;
  }

  function restorePopoverState(popover, restoreFocus) {
    var trigger = popover.__controlTrigger;

    (popover.__controlInertNodes || []).forEach(function (node) {
      node.inert = false;
    });
    popover.__controlInertNodes = [];
    popover.__controlTrigger = null;
    if (restoreFocus && trigger && trigger.isConnected) {
      trigger.focus();
      deferFocus(function () { trigger.focus(); });
    }
  }

  function initializePopover(popover) {
    if (!popover || popover.hasAttribute("data-focus-managed")) return;
    popover.setAttribute("data-focus-managed", "");
    popover.addEventListener("toggle", function (event) {
      if (event.newState === "open") {
        if (activePopover && activePopover !== popover) {
          restorePopoverState(activePopover, false);
        }
        activePopover = popover;
        setPopoverBackgroundInert(popover);
        deferFocus(function () {
          if (popover.matches(":popover-open")) focusPopover(popover);
        });
      } else if (event.newState === "closed" && activePopover === popover) {
        activePopover = null;
        restorePopoverState(popover, true);
      }
    });
  }

  function setSelectValue(select, value) {
    var option;
    if (!select) return;
    option = Array.from(select.options).find(function (candidate) {
      return candidate.value === value;
    });
    if (!option) {
      option = document.createElement("option");
      option.value = value;
      option.textContent = value;
      select.appendChild(option);
    }
    select.value = value;
  }

  function parseAclDocument(text) {
    var normalized = (text || "").replace(/\r\n?/g, "\n");
    var match = normalized.match(
      /^user ([A-Za-z0-9_.:@~-]+) (allow|deny)(?: \{\n([\s\S]*)\n\})?$/
    );
    var entries = "";
    if (!match) return null;
    if (match[3]) {
      entries = match[3].split("\n").map(function (line) {
        return line.slice(0, 2) === "  " ? line.slice(2) : line;
      }).join("\n");
    }
    return {subject: match[1], connection: match[2], entries: entries};
  }

  function setAclDocument(builder, documentValue) {
    if (!documentValue) return false;
    setSelectValue(builder.querySelector("[data-acl-subject]"), documentValue.subject);
    builder.querySelector("[data-acl-connection]").value = documentValue.connection;
    builder.querySelector("[data-acl-entries]").value = documentValue.entries;
    return true;
  }

  function updateAclDocument(builder) {
    var connection = builder.querySelector("[data-acl-connection]").value;
    var entries = builder.querySelector("[data-acl-entries]");
    var ruleInput = builder.querySelector("[data-acl-rule]");
    var subject = builder.querySelector("[data-acl-subject]").value;
    var preview = builder.querySelector("[data-acl-preview]");
    var lines;
    var rule;

    entries.disabled = connection === "deny";
    if (!subject) {
      ruleInput.value = "";
      preview.textContent = "Select a user.";
      return false;
    }
    lines = entries.value.replace(/\r\n?/g, "\n").split("\n").map(function (line) {
      return line.trim();
    }).filter(Boolean);
    rule = "user " + subject + " " + connection;
    if (connection === "allow" && lines.length) {
      rule += " {\n" + lines.map(function (line) { return "  " + line; }).join("\n") + "\n}";
    }
    ruleInput.value = rule;
    preview.textContent = rule;
    return true;
  }

  function initializeAclBuilders(target) {
    var template = document.getElementById("acl-builder-template");
    if (!target || !template) return;
    target.querySelectorAll("[data-acl-builder-host]").forEach(function (host) {
      var builder;
      var current;
      if (host.hasAttribute("data-loaded")) return;
      host.appendChild(template.content.cloneNode(true));
      host.setAttribute("data-loaded", "");
      builder = host.querySelector("[data-acl-builder]");
      builder.setAttribute("data-domain", host.getAttribute("data-domain") || "");
      current = host.getAttribute("data-current-rule");
      if (current) setAclDocument(builder, parseAclDocument(current));
      updateAclDocument(builder);
    });
  }

  function createRequestId() {
    var bytes;
    if (typeof window.crypto.randomUUID === "function") {
      return "dashboard-" + window.crypto.randomUUID();
    }
    bytes = new Uint8Array(16);
    window.crypto.getRandomValues(bytes);
    return "dashboard-" + Array.from(bytes, function (value) {
      return value.toString(16).padStart(2, "0");
    }).join("");
  }

  function selectCredentialToken(secret) {
    var token = secret ? secret.querySelector("[data-credential-token]") : null;
    var selection;
    var range;

    if (!token) return;
    token.focus();
    if (typeof window.getSelection !== "function" || typeof document.createRange !== "function") return;
    selection = window.getSelection();
    range = document.createRange();
    range.selectNodeContents(token);
    selection.removeAllRanges();
    selection.addRange(range);
  }

  function focusCredentialSecret(target) {
    var secret = target ? target.querySelector("[data-credential-secret]") : null;
    if (!secret) return;
    deferFocus(function () {
      if (secret.isConnected) secret.focus();
    });
  }

  document.addEventListener("click", function (event) {
    var option = event.target.closest("[data-picker-option]");
    var popoverButton = event.target.closest("[popovertarget]");
    var copyButton = event.target.closest("[data-copy-credential]");
    var dismissButton = event.target.closest("[data-dismiss-credential]");
    var secret;
    var token;
    var status;
    var popover;

    if (dismissButton) {
      secret = dismissButton.closest("[data-credential-secret]");
      token = secret ? secret.querySelector("[data-credential-token]") : null;
      if (token) token.textContent = "";
      if (secret) secret.remove();
      return;
    }
    if (copyButton) {
      secret = copyButton.closest("[data-credential-secret]");
      token = secret ? secret.querySelector("[data-credential-token]") : null;
      status = secret ? secret.querySelector("[data-credential-copy-status]") : null;
      if (!token || !status) return;
      if (navigator.clipboard && typeof navigator.clipboard.writeText === "function") {
        navigator.clipboard.writeText(token.textContent).then(function () {
          status.textContent = "Token copied.";
        }).catch(function () {
          selectCredentialToken(secret);
          status.textContent = "Select and copy the token manually.";
        });
      } else {
        selectCredentialToken(secret);
        status.textContent = "Select and copy the token manually.";
      }
      return;
    }

    if (popoverButton) {
      popover = document.getElementById(popoverButton.getAttribute("popovertarget"));
      if (popover) {
        initializePopover(popover);
        if (!popover.contains(popoverButton)) popover.__controlTrigger = popoverButton;
        loadPickerOptions(popover);
        initializeAclBuilders(popover);
      }
    }
    if (option) selectOption(option);
  });

  document.addEventListener("input", function (event) {
    var builder = event.target.closest("[data-acl-builder]");
    resetRequestIds(event.target.closest(".command"));
    if (builder) updateAclDocument(builder);
  });

  document.addEventListener("change", function (event) {
    var builder = event.target.closest("[data-acl-builder]");
    resetRequestIds(event.target.closest(".command"));
    if (builder) updateAclDocument(builder);
  });

  document.addEventListener("keydown", function (event) {
    var option = event.target.closest("[data-picker-option]");
    var nextOption;
    var focusable;
    var first;
    var last;

    if (option && !option.disabled) {
      nextOption = adjacentPickerOption(option, event.key);
      if (nextOption) {
        event.preventDefault();
        selectOption(nextOption);
        nextOption.focus();
        return;
      }
    }

    if (!activePopover || !activePopover.matches(":popover-open") || event.key !== "Tab") return;
    focusable = getFocusable(activePopover);
    if (!focusable.length) return;
    first = focusable[0];
    last = focusable[focusable.length - 1];
    if (!activePopover.contains(document.activeElement)) {
      event.preventDefault();
      first.focus();
    } else if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus();
    }
  });

  document.addEventListener("htmx:beforeSwap", function () {
    if (!activePopover) return;
    restorePopoverState(activePopover, false);
    activePopover = null;
  });

  document.addEventListener("htmx:afterSwap", function (event) {
    focusCredentialSecret(event.detail.target);
  });

  document.addEventListener("htmx:configRequest", function (event) {
    var command = event.detail.elt.closest(".command");
    var requestId;
    var ruleBuilder;
    var ruleInput;

    if (!command) return;
    ruleBuilder = command.querySelector("[data-acl-builder]");
    if (ruleBuilder && !updateAclDocument(ruleBuilder)) {
      ruleBuilder.querySelector("[data-acl-subject]").reportValidity();
      event.preventDefault();
      return;
    }
    requestId = command.querySelector("[data-request-id]");
    if (requestId) {
      if (!requestId.value) requestId.value = createRequestId();
      event.detail.parameters.request_id = requestId.value;
    }
    ruleInput = command.querySelector("[data-acl-rule]");
    if (ruleInput) event.detail.parameters.rule_line = ruleInput.value;
  });
}());

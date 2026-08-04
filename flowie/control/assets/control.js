(function () {
  "use strict";

  var ACL_ACTION_LABELS = {
    connect: "Connect",
    publish: "Publish",
    subscribe: "Subscribe",
    read: "Read",
    write: "Write",
    execute: "Execute",
    admin: "Admin"
  };
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

  function splitRuleLine(line) {
    var fields = [];
    var field = "";
    var index = 0;
    var hex;
    var next;

    while (index < line.length) {
      if (line[index] === "|") {
        fields.push(field);
        field = "";
        index += 1;
        continue;
      }
      if (line[index] !== "\\") {
        field += line[index];
        index += 1;
        continue;
      }
      next = line[index + 1];
      if (next === "\\" || next === "|") {
        field += next;
        index += 2;
        continue;
      }
      hex = line.slice(index + 2, index + 4);
      if (next !== "x" || !/^[0-9a-fA-F]{2}$/.test(hex)) return null;
      field += String.fromCharCode(parseInt(hex, 16));
      index += 4;
    }
    fields.push(field);
    return fields.length === 8 ? fields : null;
  }

  function escapeRuleField(value) {
    return value.replace(/\\/g, "\\\\").replace(/\|/g, "\\|").replace(
      /[\u0000-\u001f\u007f]/g,
      function (character) {
        return "\\x" + character.charCodeAt(0).toString(16).padStart(2, "0");
      }
    );
  }

  function setSelectValue(select, value, label) {
    var option;
    if (!select) return;
    option = Array.from(select.options).find(function (candidate) {
      return candidate.value === value;
    });
    if (!option) {
      option = document.createElement("option");
      option.value = value;
      option.textContent = label || value;
      select.appendChild(option);
    }
    select.value = value;
  }

  function setAclFields(builder, fields) {
    var actions;
    var subjectValue;
    if (!fields) return false;
    subjectValue = fields[1] + ":" + (fields[1] === "any" ? "*" : fields[2]);
    setSelectValue(builder.querySelector("[data-acl-effect]"), fields[0]);
    setSelectValue(builder.querySelector("[data-acl-subject]"), subjectValue,
                   fields[1] + " · " + fields[2]);
    actions = fields[4].split(",");
    builder.querySelectorAll("[data-acl-action]").forEach(function (checkbox) {
      checkbox.checked = actions.indexOf(checkbox.value) !== -1;
    });
    setSelectValue(builder.querySelector("[data-acl-resource]"), fields[5]);
    setSelectValue(builder.querySelector("[data-acl-match]"), fields[6]);
    builder.querySelector("[data-acl-pattern]").value = fields[7];
    return true;
  }

  function applyAclExample(builder, example) {
    var root = builder.getAttribute("data-domain");
    var fields = ["allow", "any", "*", root, "subscribe", "mqtt_topic", "adapter",
                  root + "/events/#"];
    if (example === "publish-events") {
      fields[4] = "publish";
      fields[7] = root + "/+/events/#";
    } else if (example === "deny-private") {
      fields[0] = "deny";
      fields[7] = root + "/private/#";
    }
    setAclFields(builder, fields);
    updateAclRule(builder);
  }

  function updateAclRule(builder) {
    var actionLabels = [];
    var actions = [];
    var effect = builder.querySelector("[data-acl-effect]").value;
    var match = builder.querySelector("[data-acl-match]").value;
    var patternInput = builder.querySelector("[data-acl-pattern]");
    var resource = builder.querySelector("[data-acl-resource]").value;
    var root = builder.getAttribute("data-domain");
    var ruleInput = builder.querySelector("[data-acl-rule]");
    var subject = builder.querySelector("[data-acl-subject]").value;
    var subjectSeparator = subject.indexOf(":");
    var subjectKind = subject.slice(0, subjectSeparator);
    var subjectValue = subject.slice(subjectSeparator + 1);
    var preview = builder.querySelector("[data-acl-preview]");

    builder.querySelectorAll("[data-acl-action]:checked").forEach(function (checkbox) {
      actions.push(checkbox.value);
      actionLabels.push(ACL_ACTION_LABELS[checkbox.value] || checkbox.value);
    });
    patternInput.setCustomValidity(actions.length ? "" : "Select at least one operation.");
    if (!actions.length || !patternInput.value) {
      ruleInput.value = "";
      preview.textContent = actions.length ? "Enter a resource path." : "Select an operation.";
      return false;
    }
    ruleInput.value = [
      effect,
      subjectKind,
      subjectKind === "any" ? "*" : escapeRuleField(subjectValue),
      escapeRuleField(root),
      actions.join(","),
      resource,
      match,
      escapeRuleField(patternInput.value)
    ].join("|");
    preview.textContent =
      (effect === "allow" ? "Allow" : "Deny") + " · " +
      builder.querySelector("[data-acl-subject]").selectedOptions[0].textContent.trim() + " · " +
      actionLabels.join(", ") + " · " + patternInput.value;
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
      if (!current || !setAclFields(builder, splitRuleLine(current))) {
        applyAclExample(builder, "subscribe-events");
      } else {
        updateAclRule(builder);
      }
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

  document.addEventListener("click", function (event) {
    var example = event.target.closest("[data-acl-example]");
    var option = event.target.closest("[data-picker-option]");
    var popoverButton = event.target.closest("[popovertarget]");
    var popover;

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
    if (example) applyAclExample(example.closest("[data-acl-builder]"),
                                 example.getAttribute("data-acl-example"));
  });

  document.addEventListener("input", function (event) {
    var builder = event.target.closest("[data-acl-builder]");
    resetRequestIds(event.target.closest(".command"));
    if (builder) updateAclRule(builder);
  });

  document.addEventListener("change", function (event) {
    var builder = event.target.closest("[data-acl-builder]");
    resetRequestIds(event.target.closest(".command"));
    if (builder) updateAclRule(builder);
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

  document.addEventListener("htmx:configRequest", function (event) {
    var command = event.detail.elt.closest(".command");
    var requestId;
    var ruleBuilder;
    var ruleInput;

    if (!command) return;
    ruleBuilder = command.querySelector("[data-acl-builder]");
    if (ruleBuilder && !updateAclRule(ruleBuilder)) {
      ruleBuilder.querySelector("[data-acl-pattern]").reportValidity();
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

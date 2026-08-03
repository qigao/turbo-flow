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

  function resetRequestIds(scope) {
    if (!scope) return;
    scope.querySelectorAll("[data-request-id]").forEach(function (requestId) {
      requestId.value = "";
    });
  }

  function selectOption(option) {
    var picker = option.closest("[data-entity-picker]");
    var labelNode;
    var value;
    var label;

    if (!picker || option.disabled) return;
    value = option.getAttribute("data-value") || "";
    labelNode = option.querySelector("span");
    label = (labelNode ? labelNode.textContent : option.textContent).trim();
    picker.querySelectorAll("[data-picker-option]").forEach(function (candidate) {
      candidate.setAttribute("aria-selected", candidate === option ? "true" : "false");
    });
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
    var root = builder.getAttribute("data-root-group");
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
    var root = builder.getAttribute("data-root-group");
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
      builder.setAttribute("data-root-group", host.getAttribute("data-root-group") || "");
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
      loadPickerOptions(popover);
      initializeAclBuilders(popover);
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

// Add your javascript here

$(function () {
  
  $('#builder').queryBuilder({
    plugins: ['bt-tooltip-errors'],

    filters: [{
      id: 'variant_name',
      label: 'variant_name - variation',
      type: 'string'

    }, {
      id: 'variant_id',
      label: 'variant_id - variation',
      type: 'string'

    }, {
      id: 'chromosome',
      label: 'chromosome - variation',
      type: 'string'

    }, {
      id: 'gene',
      label: 'gene - gene',
      type: 'string'

    }, {
      id: 'variant_type',
      label: 'variant_type - variation',
      type: 'string'

    }, {
      id: 'description',
      label: 'description - variation',
      type: 'string'

    }, {
      id: 'polyphen_prediction',
      label: 'polyphen_prediction - variation',
      type: 'string'

    }, {
      id: 'sift_prediction',
      label: 'sift_prediction - variation',
      type: 'string'

    }, {
      id: 'hgvs',
      label: 'hgvs - hgvs',
      type: 'string'

    }, {
      id: 'assembly',
      label: 'assembly - assembly',
      type: 'string'

    }, {
      id: 'assembly_date',
      label: 'date - assembly',
      type: 'string'

    }, {
      id: 'start',
      label: 'start - assembly',
      type: 'integer'

    }, {
      id: 'end',
      label: 'end - assembly',
      type: 'integer'

    }, {
      id: 'ref',
      label: 'ref - assembly',
      type: 'string'

    }, {
      id: 'alt',
      label: 'alt - assembly',
      type: 'string'

    }, {
      id: 'risk_allele',
      label: 'risk_allele - assembly',
      type: 'string'

    }, {
      id: 'phenotype',
      label: 'phenotype - phenotype',
      type: 'string'

    }, {
      id: 'clinical_actionability',
      label: 'clinical_actionability - phenotype',
      type: 'string'

    }, {
      id: 'classification',
      label: 'classification - phenotype',
      type: 'string'

    }, {
      id: 'clinical_significance',
      label: 'clinical_significance - interpretation',
      type: 'string'

    }, {
      id: 'method',
      label: 'method - interpretation',
      type: 'string'

    }, {
      id: 'assertion_criteria',
      label: 'assertion_criteria - interpretation',
      type: 'string'

    }, {
      id: 'level_certainty',
      label: 'level_certainty - interpretation',
      type: 'string'

    }, {
      id: 'date',
      label: 'date - interpretation',
      type: 'string'

    }, {
      id: 'author',
      label: 'author - interpretation',
      type: 'string'

    }, {
      id: 'origin',
      label: 'origin - interpretation',
      type: 'string'

    }, {
      id: 'title',
      label: 'title - bibliography',
      type: 'string'

    }, {
      id: 'year',
      label: 'year - bibliography',
      type: 'integer'

    }, {
      id: 'authors',
      label: 'authors - bibliography',
      type: 'string'

    }, {
      id: 'pmid',
      label: 'pmid - bibliography',
      type: 'integer'

    }, {
      id: 'is_gwas',
      label: 'is_gwas - bibliography',
      type: 'boolean'

    }, {
      id: 'name',
      label: 'name - databank',
      type: 'string'

    }, {
      id: 'url',
      label: 'url - databank',
      type: 'string'

    }, {
      id: 'version',
      label: 'version - databank',
      type: 'string'

    }, {
      id: 'databanks_variant_id',
      label: 'variant_id - databank',
      type: 'string'

    }, {
      id: 'clinvar_accession',
      label: 'clinvar_accession - databank',
      type: 'string'

    },

    ]
  });
});
/**

$('#btn-reset').on('click', function () {
  $('#builder').queryBuilder('reset');
});

$('#btn-get').on('click', function () {
  var result = $('#builder').queryBuilder('getRules');

  if (!$.isEmptyObject(result)) {
    alert(JSON.stringify(result, null, 2));
  }
});


$('#btn-get').on('click', function() {
	var result = $('#builder').queryBuilder('getRules');
	if (!$.isEmptyObject(result)) {
		alert(JSON.stringify(result, null, 2));
	}
	else{
		console.log("invalid object :");
	}
	console.log(result);
});


$('#btn-reset').on('click', function () {
  $('#builder').queryBuilder('reset');
});


*/

/**
 * @class NotGroup 
 * @memberof module:plugins
 * @description Adds a "Not" checkbox in front of group conditions.
 * @param {object} [options]
 * @param {string} [options.icon_checked='glyphicon glyphicon-checked']
 * @param {string} [options.icon_unchecked='glyphicon glyphicon-unchecked']
 */
 QueryBuilder.define('not-group', function(options) {
  var self = this;

  // Bind events
  this.on('afterInit', function() {
      self.$el.on('click.queryBuilder', '[data-not=group]', function() {
          var $group = $(this).closest(QueryBuilder.selectors.group_container);
          var group = self.getModel($group);
          group.not = !group.not;
      });

      self.model.on('update', function(e, node, field) {
          if (node instanceof Group && field === 'not') {
              self.updateGroupNot(node);
          }
      });
  });

  // Init "not" property
  this.on('afterAddGroup', function(e, group) {
      group.__.not = false;
  });

  // Modify templates
  if (!options.disable_template) {
      this.on('getGroupTemplate.filter', function(h) {
          var $h = $(h.value);
          $h.find(QueryBuilder.selectors.condition_container).prepend(
              '<button type="button" class="btn btn-xs btn-default" data-not="group">' +
              '<i class="' + options.icon_unchecked + '"></i> ' + self.translate('NOT') +
              '</button>'
          );
          h.value = $h.prop('outerHTML');
      });
  }

  // Export "not" to JSON
  this.on('groupToJson.filter', function(e, group) {
      e.value.not = group.not;
  });

  // Read "not" from JSON
  this.on('jsonToGroup.filter', function(e, json) {
      e.value.not = !!json.not;
  });

  // Export "not" to SQL
  this.on('groupToSQL.filter', function(e, group) {
      if (group.not) {
          e.value = 'NOT ( ' + e.value + ' )';
      }
  });

  // Parse "NOT" function from sqlparser
  this.on('parseSQLNode.filter', function(e) {
      if (e.value.name && e.value.name.toUpperCase() == 'NOT') {
          e.value = e.value.arguments.value[0];

          // if the there is no sub-group, create one
          if (['AND', 'OR'].indexOf(e.value.operation.toUpperCase()) === -1) {
              e.value = new SQLParser.nodes.Op(
                  self.settings.default_condition,
                  e.value,
                  null
              );
          }

          e.value.not = true;
      }
  });

  // Request to create sub-group if the "not" flag is set
  this.on('sqlGroupsDistinct.filter', function(e, group, data, i) {
      if (data.not && i > 0) {
          e.value = true;
      }
  });

  // Read "not" from parsed SQL
  this.on('sqlToGroup.filter', function(e, data) {
      e.value.not = !!data.not;
  });

}, {
  icon_unchecked: 'glyphicon glyphicon-unchecked',
  icon_checked: 'glyphicon glyphicon-check',
  disable_template: false
});

/**
* From {@link module:plugins.NotGroup}
* @name not
* @member {boolean}
* @memberof Group
* @instance
*/
Utils.defineModelProperties(Group, ['not']);

QueryBuilder.selectors.group_not = QueryBuilder.selectors.group_header + ' [data-not=group]';

QueryBuilder.extend(/** @lends module:plugins.NotGroup.prototype */ {
  /**
   * Performs actions when a group's not changes
   * @param {Group} group
   * @fires module:plugins.NotGroup.afterUpdateGroupNot
   * @private
   */
  updateGroupNot: function(group) {
      var options = this.plugins['not-group'];
      group.$el.find('>' + QueryBuilder.selectors.group_not)
          .toggleClass('active', group.not)
          .find('i').attr('class', group.not ? options.icon_checked : options.icon_unchecked);

      /**
       * After the group's not flag has been modified
       * @event afterUpdateGroupNot
       * @memberof module:plugins.NotGroup
       * @param {Group} group
       */
      this.trigger('afterUpdateGroupNot', group);

      this.trigger('rulesChanged');
  }
});
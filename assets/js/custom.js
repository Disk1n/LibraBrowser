(function ($) {
	"use strict";


	$(document).ready(function(){

		welcome();

		// Menu Dropdown Toggle
		if($('.menu-trigger').length){
			$('.menu-trigger').click(function(){
				$(this).toggleClass('active');
				$('.header-area .nav').slideToggle(200);
			});
		}

		// Language Flags Dropdown Toggle
		$('body').click(function(e){
			var el = e.target;
	
			if($(el).parents('.flag-list').length || $(el).hasClass('flag-list')) return; 
	
			if($('.flag-list').css('display') === 'block') {
				$('.flag-list').css('display', 'none');
				return;
			}
	
			if( $(el).hasClass('selected') || $(el).parents('.selected').length) {
				$('.flag-list').css('display', 'block');
			}
		});



		// Countdown init
		if($('.countdown').length){
			$('.countdown').downCount({
				date: '07/05/2018 12:00:00',
				offset: +10
			});
		}	


		// Token input init
		if($('.token .token-input').length){
			$('.token .token-input .fa-plus').click(function(){
				var val = $(this).parent().find('input').val();
				var step = $(this).parent().find('input').data('step');
				if(val == '') {
					val = 0;
				}
				var newVal = parseInt(val, 10) + parseInt(step, 10);
				$(this).parent().find('input').val(newVal);
			});
			$('.token .token-input .fa-minus').click(function(){
				var val = $(this).parent().find('input').val();
				var step = $(this).parent().find('input').data('step');
				if(val == '') {
					val = 0;
				}
				var newVal = parseInt(val, 10) - parseInt(step, 10);
				if(newVal <= 0) {
					newVal = step;
				}
				$(this).parent().find('input').val(newVal);
			});
		}

		// Scroll animation init
		window.sr = new scrollReveal();


		// Menu elevator animation
		$('a[href*=\\#]:not([href=\\#])').click(function() {
			if (location.pathname.replace(/^\//,'') == this.pathname.replace(/^\//,'') && location.hostname == this.hostname) {
				var target = $(this.hash);
				target = target.length ? target : $('[name=' + this.hash.slice(1) +']');
				if (target.length) {
					var width = $(window).width();
					if(width < 991) {
						$('.menu-trigger').removeClass('active');
						$('.header-area .nav').slideUp(200);	
					}				
					$('html,body').animate({
						scrollTop: (target.offset().top) - 30
					}, 700);
					return false;
				}
			}
		});	


		// Token Progress Bar
		if($('.token-progress ul').length){
			$(".token-progress ul").find(".item").each(function(i){
				$('.token-progress ul .item:eq(' +[i]+ ')').css("left", $('.token-progress ul .item:eq(' + [i] + ')').data('position'));
			});
			var progress = $(".token-progress ul .progress-active").data('progress');
			$(".token-progress ul .progress-active").css('width', progress);
		}

		// Rich List Progress
		if($('.table-progress').length){
			$(".table-latests").find(".table-progress").each(function(i){
				$('.table-progress:eq(' +[i]+ ') .progress-line').css("width", parseInt($('.table-progress:eq(' +[i]+ ') .progress-line').data('value'),10) + parseInt(70, 10)  + '%');
			});
		}


		// Roadmap Carousel Init
		if($('.roadmap-modern-wrapper').length){
			$('.roadmap-modern-wrapper').owlCarousel({
				loop:true,
				margin:30,
				nav:false,
				responsive:{
					0:{
						items:1
					},
					600:{
						items:2
					},
					1000:{
						items:3
					}
				}
			});
		}


		// Roadmap Carousel Init
		if($('.roadmap-lux-wrapper').length){
			$('.roadmap-lux-wrapper').owlCarousel({
				loop:true,
				margin:30,
				nav:false,
				responsive:{
					0:{
						items:1
					},
					600:{
						items:2
					},
					1000:{
						items:3
					}
				}
			});
		}

	});


	// Page loading animation
	$(window).load(function(){
		$(".loading-wrapper").animate({
			'opacity': '0'
		}, 600, function(){
			setTimeout(function(){
				$(".loading-wrapper").css("visibility", "hidden").fadeOut();

				// Parallax init
				if($('.parallax').length){
					$('.parallax').parallax({
						imageSrc: 'assets/images/parallax.jpg',
						zIndex: '1'
					});
				}
			}, 300);
		});
	});



	// Header Scrolling Set White Background
	$(window).scroll(function() {
		var width = $(window).width();
		if(width > 991) {
			var scroll = $(window).scrollTop();
			if (scroll >= 30) {
				$(".header-area").addClass("header-sticky");
				$(".header-area .dark-logo").css('display', 'block');
				$(".header-area .light-logo").css('display', 'none');
			}else{
				$(".header-area").removeClass("header-sticky");
				$(".header-area .dark-logo").css('display', 'none');
				$(".header-area .light-logo").css('display', 'block');
			}
		}
	});


	// Window resize setting
	$(window).resize(function(){
		welcome();
	});


	// Welcome area height settings
	function welcome() {
		var width = $(window).width();

		if(width > 991) {
			var height = $(window).height();
			$('.welcome-area').css('height', height - 80);
		}else{
			$('.welcome-area').css('height', 'auto');
		}

		// Welcome1 particleJS init
		if($('#welcome-1').length){
			particlesJS('welcome-1', welcome1Settings);
		}
	}

})(jQuery);
